defmodule LogParser do
  @moduledoc """
  Documentation for LogParser.
  """
  @path "/home/bazaretas/projects/exparser/production.log.cp"
  @doc """
  Different techniques for parsing a 600MB file
  with about  4_200_000 lines

  # 281808770,  @home -> second run
  # 287168128,  @home -> chunk and task
  # 335518642,  @home -> re-run
  # 278888883,  @home -> re-run
  # 262360238,  @home -> flow
  # 13115862,   @home -> increase the cache, result not correct!
  # 2697926,    @home -> increase the cache, with correct result
  """
  def parse_with_flow do
    File.stream!(@path)
    |> Flow.from_enumerable()
    |> Flow.filter(fn line ->
      list?(line) || answer?(line)
    end)
    |> Flow.partition()
    |> Enum.to_list
  end

  def parse_with_stream do
    File.stream!(@path, [], 128 * 4096)
    |> Stream.filter(fn line ->
      list?(line) || answer?(line)
    end)
    |> Enum.to_list
    |> Enum.count
  end

  def parse do
    File.stream!(@path, [], 128 * 4096)
    |> Enum.reduce([], fn lines, acc ->
      [Task.async(fn->filter(lines)end) | acc]
    end)
    |> Enum.map(&Task.await/1)
    |> List.flatten
    |> IO.inspect
    |> Enum.count
  end

  def filter(lines) do
    String.split(lines, "\n")
    |> Enum.filter(fn(line)->
      list?(line) || answer?(line)
    end)
  end

  def parse_with_task do
    File.stream!(@path)
    |> Enum.filter(&Task.async(fn ->
      list?(&1) || answer?(&1)
    end))
    |> Enum.map(&Task.await/1)
    |> Enum.to_list
  end

  defp list?(line) do
    Regex.match?(~r/list\.json/, line)
  end

  defp answer?(line) do
    Regex.match?(~r/"answer"=\>\{"rate"/, line)
  end


end

# 248601509 -> stream
# 235091281 -> flow
