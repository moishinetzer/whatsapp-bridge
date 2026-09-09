import { FetchHttpClient } from "effect/unstable/http"
import { NodeServices } from "@effect/platform-node"
import { Layer } from "effect"
import { BridgeConfigLive, DotEnvLive, type BridgeConfigShape } from "./config.ts"
import { EventsLive } from "./engine/Events.ts"
import { HealthLive } from "./engine/Health.ts"
import { ResponderLive } from "./engine/Responder.ts"
import { TurnEngineLive, TurnRegistryLive } from "./engine/Turns.ts"
import { HttpServerLive } from "./http/Server.ts"
import { LoggingLive } from "./logging.ts"
import { AuthStoreLive } from "./whatsapp/AuthStore.ts"
import { BaileysLive, type BaileysOptions } from "./whatsapp/Baileys.ts"

/** Environment variables, then `.env`, on top of the Node platform services. */
export const EnvLive = DotEnvLive.pipe(Layer.provide(NodeServices.layer))

/** Loggers and the log threshold. Provided under everything so every fiber inherits them. */
export const AmbientLive = LoggingLive.pipe(Layer.provide(EnvLive))

/**
 * The services every command shares: config, health, events, the session
 * store and the WhatsApp transport. `mode` decides whether the transport links
 * (`bridge link`) or serves.
 */
export const coreLayer = (transport: BaileysOptions, overrides: Partial<BridgeConfigShape> = {}) => {
  const Config = BridgeConfigLive(overrides).pipe(Layer.provide(EnvLive))
  const Health = HealthLive.pipe(Layer.provide(Config))
  const Events = EventsLive.pipe(Layer.provide(Health))
  const Auth = AuthStoreLive.pipe(Layer.provide([Config, NodeServices.layer]))
  const WhatsApp = BaileysLive(transport).pipe(Layer.provide([Config, Auth, Health, Events]))
  return Layer.mergeAll(Config, Health, Events, Auth, WhatsApp)
}

/** The daemon: transport + turn engine + local HTTP API. */
export const daemonLayer = (overrides: Partial<BridgeConfigShape> = {}) => {
  const Core = coreLayer({ mode: "serve" }, overrides)
  const Registry = TurnRegistryLive
  const Responder = ResponderLive.pipe(Layer.provide([Core, FetchHttpClient.layer]))
  const Engine = TurnEngineLive.pipe(Layer.provide([Core, Registry, Responder]))
  const Server = HttpServerLive.pipe(Layer.provide([Core, Registry]))
  return Layer.mergeAll(Core, Registry, Responder, Engine, Server).pipe(Layer.provideMerge(AmbientLive))
}

/** `bridge link`: the transport in link mode, nothing else. */
export const linkLayer = (phone: string | undefined) =>
  coreLayer({ mode: "link", phone }).pipe(Layer.provideMerge(AmbientLive))
