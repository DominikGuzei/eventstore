defmodule EventStore.Supervisor do
  use Supervisor

  def start_link(config) do
    serializer = EventStore.configured_serializer()

    Supervisor.start_link(__MODULE__, [config, serializer])
  end

  def init([config, serializer]) do
    children = [
      supervisor(Registry, [:unique, EventStore.Streams], id: :streams_registry),
      supervisor(Registry, [:unique, EventStore.Subscriptions], id: :subscriptions_registry),
      supervisor(Registry, [:duplicate, EventStore.Subscriptions.PubSub, [partitions: System.schedulers_online]], id: :subscriptions_pubsub_registry),
      supervisor(EventStore.Storage.PoolSupervisor, [config]),
      supervisor(EventStore.Subscriptions.Supervisor, []),
      supervisor(EventStore.Streams.Supervisor, [serializer]),
    ]

    supervise(children, strategy: :one_for_one)
  end
end
