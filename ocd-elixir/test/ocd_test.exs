defmodule OcdTest do
  use ExUnit.Case
  doctest Ocd

  test "greets the world" do
    assert Ocd.hello() == :world
  end
end
