use Mix.Config

# Print only warnings and errors during test
config :logger, :console, level: :debug, format: "[$level] $message\n"

config :ex_unit, capture_log: true

config :eventstore, EventStore.Storage,
  serializer: EventStore.JsonSerializer,
  username: "postgres",
  password: "postgres",
  database: "eventstore_test",
  hostname: "localhost",
  pool_size: 1
