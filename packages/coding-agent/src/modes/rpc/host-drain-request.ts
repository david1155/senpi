/**
 * The drain REQUEST: the signal-free way to ask one named generation to drain, on every platform
 * (senpi#3653).
 *
 * A drain used to be SIGUSR1 and nothing else, so a win32 generation - which has no such signal -
 * could never be asked to leave: it held its retained sessions and their path claims until it idled
 * out or died. The request is a file in the TARGET generation's own directory instead, polled by the
 * process that should act on it:
 *
 * - `drain-request.json` is written by whoever asks (`host stop --drain`), and read by the
 *   generation's supervisor. Like the stop intent (`host-stop-intent.ts`) it is a sanctioned
 *   cross-writer of generation state (rpc `AGENTS.md`, I3): the asker writes into the target's
 *   directory because the target cannot know who is about to ask.
 * - `host-drain-request.json` is written only by that supervisor and read only by its host child:
 *   the supervisor's relay of the drain to the process holding the sessions.
 *
 * A generation advertises that it reads requests through `drain-watch.json`, which names the
 * supervisor pid that armed the watch. An asker trusts that marker only together with an identity
 * proof of that same pid, so a marker left behind by a dead process authorizes nothing.
 *
 * Every request carries a fresh nonce, and a watcher delivers each distinct request once: asking
 * again is a new request (the supervisor's grace expiry re-asks its child), seeing the same file
 * again is not. An unreadable file is ignored rather than read as a request - a torn or foreign
 * write must never be what drains a generation. The file is never removed by the reader: the
 * directory belongs to one generation and leaves with it.
 */
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import type { HostStopSender } from "./host-crash-record.ts";
import { HOST_DAEMON_DIR_ENV, hostDaemonDirectoryPaths } from "./host-daemon-paths.ts";
import { generationPaths } from "./host-generation-paths.ts";
import { HOST_INSTANCE_ID_ENV } from "./host-identity-env.ts";
import { readJsonObject, writeJsonAtomic } from "./host-state-json.ts";

/** How often a generation looks for a drain request: the cadence of the supersession watch. */
export const DRAIN_REQUEST_POLL_MS = 1_000;

export interface HostDrainRequest {
	readonly sender: HostStopSender;
	readonly reason: string;
	readonly at: string;
	/** Distinguishes one request from the next; a watcher delivers each nonce once. */
	readonly nonce: string;
}

/** Records a drain request for whoever polls `file`. Atomic: a reader sees no request or a whole one. */
export async function writeDrainRequest(
	file: string,
	request: { readonly sender: HostStopSender; readonly reason: string },
): Promise<void> {
	const record: HostDrainRequest = { ...request, at: new Date().toISOString(), nonce: randomUUID() };
	await writeJsonAtomic(file, record);
}

/**
 * Calls `onRequest` for every distinct request that appears at `file`, including one already there
 * when the watch arms. Unref'd: a drain watch must never be the reason a process stays up.
 */
export function watchDrainRequest(
	file: string,
	onRequest: (request: HostDrainRequest) => void,
	options: { readonly pollMs?: number } = {},
): () => void {
	let stopped = false;
	let polling = false;
	let lastNonce: string | undefined;
	const poll = (): void => {
		if (stopped || polling) return;
		polling = true;
		void readDrainRequest(file)
			.then((request) => {
				if (stopped || request === undefined || request.nonce === lastNonce) return;
				lastNonce = request.nonce;
				onRequest(request);
			}, ignore)
			.finally(() => {
				polling = false;
			});
	};
	const timer = setInterval(poll, options.pollMs ?? DRAIN_REQUEST_POLL_MS);
	timer.unref?.();
	poll();
	return () => {
		stopped = true;
		clearInterval(timer);
	};
}

/** The request at `file`, or nothing when it is absent or does not parse as one. */
export async function readDrainRequest(file: string): Promise<HostDrainRequest | undefined> {
	const raw = await readJsonObject(file);
	if (raw === undefined) return undefined;
	const sender = raw.sender;
	if (typeof sender !== "object" || sender === null) return undefined;
	const pid = Reflect.get(sender, "pid");
	const kind = Reflect.get(sender, "kind");
	if (typeof pid !== "number" || typeof kind !== "string") return undefined;
	if (typeof raw.reason !== "string" || typeof raw.at !== "string" || typeof raw.nonce !== "string") return undefined;
	return {
		sender: { pid, kind: kind as HostStopSender["kind"] },
		reason: raw.reason,
		at: raw.at,
		nonce: raw.nonce,
	};
}

/** What a supervisor publishes once its watch is armed: the pid an asker must prove before asking. */
export interface HostDrainWatch {
	readonly pid: number;
}

export async function writeDrainWatch(file: string, watch: HostDrainWatch): Promise<void> {
	await writeJsonAtomic(file, { ...watch, at: new Date().toISOString() });
}

/** The advertised watcher pid, or nothing: a generation without the marker cannot be asked this way. */
export async function readDrainWatch(file: string): Promise<HostDrainWatch | undefined> {
	const raw = await readFileOrUndefined(file);
	if (raw === undefined) return undefined;
	try {
		const parsed: unknown = JSON.parse(raw);
		const pid = typeof parsed === "object" && parsed !== null ? Reflect.get(parsed, "pid") : undefined;
		return typeof pid === "number" && Number.isInteger(pid) && pid > 0 ? { pid } : undefined;
	} catch {
		return undefined;
	}
}

/**
 * The relay file a SUPERVISED host child polls, derived from what its supervisor told it at spawn
 * (daemon directory + instance id). A host started bare has no supervisor to relay anything.
 */
export function hostChildDrainRequestFile(env: NodeJS.ProcessEnv = process.env): string | undefined {
	const daemonDir = env[HOST_DAEMON_DIR_ENV];
	const instanceId = env[HOST_INSTANCE_ID_ENV];
	if (!daemonDir || !instanceId) return undefined;
	return generationPaths(hostDaemonDirectoryPaths(daemonDir), instanceId).childDrainRequestFile;
}

async function readFileOrUndefined(file: string): Promise<string | undefined> {
	try {
		return await readFile(file, "utf8");
	} catch {
		return undefined;
	}
}

function ignore(): void {}
