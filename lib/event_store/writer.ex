defmodule EventStore.Writer do
  @moduledoc """
  Single process writer to assign a monotonically increasing id and persist events to the store
  """

  use GenServer
  require Logger

  alias EventStore.{Subscriptions,RecordedEvent,Writer}
  alias EventStore.Storage.{Appender}

  defstruct [
    conn: nil,
    serializer: nil,
  ]

  def start_link(serializer) do
    GenServer.start_link(__MODULE__, %Writer{serializer: serializer}, name: __MODULE__)
  end

  def init(%Writer{} = state) do
    storage_config = EventStore.configuration() |> EventStore.Config.parse()

    {:ok, conn} = Postgrex.start_link(storage_config)

    {:ok, %Writer{state | conn: conn}}
  end

  @doc """
  Append the given list of events to the stream
  """
  @spec append_to_stream(list(RecordedEvent.t), String.t) :: :ok | {:error, reason :: any()}
  def append_to_stream(events, stream_uuid)
  def append_to_stream([], _stream_uuid), do: :ok
  def append_to_stream(events, stream_uuid) do
    GenServer.call(__MODULE__, {:append_to_stream, events, stream_uuid})
  end

  def handle_call({:append_to_stream, events, stream_uuid}, _from, %Writer{conn: conn, serializer: serializer} = state) do
    {reply, state} = case append_events(conn, events) do
      {:ok, assigned_event_ids} ->
        events
        |> assign_event_ids(assigned_event_ids)
        |> publish_events(stream_uuid, serializer)

        {:ok, state}

      {:error, _reason} = reply -> {reply, state}
    end

    {:reply, reply, state}
  end

  defp append_events(conn, events), do: Appender.append(conn, events)
  
  defp assign_event_ids(events, ids) do
    events
    |> Enum.zip(ids)
    |> Enum.map(fn {event, id} ->
      %RecordedEvent{event | event_id: id}
    end)
  end

  defp publish_events(events, stream_uuid, serializer) do
    Subscriptions.notify_events(stream_uuid, events, serializer)
  end
end
