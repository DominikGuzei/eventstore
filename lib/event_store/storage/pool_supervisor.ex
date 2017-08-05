defmodule EventStore.Storage.PoolSupervisor do
  use Supervisor

  @storage_pool_name :event_store_storage_pool

  @defaults [
    {:name, {:local, @storage_pool_name}},
    {:worker_module, Postgrex},
    {:size, 10},
    {:max_overflow, 5}
  ]

  def start_link(config) do
    Supervisor.start_link(__MODULE__, [config])
  end

  def init([config]) do
    children = [
      :poolboy.child_spec(@storage_pool_name, pool_opts(config), postgrex_opts(config))
    ]

    supervise(children, strategy: :one_for_one)
  end

  defp pool_opts(config) do
    opts = [
      size:         config[:pool_size],
      max_overflow: config[:pool_max_overflow]
    ] |> Enum.filter(fn {_,v} -> !is_nil(v) end)

    Keyword.merge(@defaults, opts)
  end

  defp postgrex_opts(config) do
    Keyword.take(config, [:username, :password, :database, :hostname, :port])
  end
end
