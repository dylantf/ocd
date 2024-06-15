defmodule Ocd.Db.AuditLog do
  use Ecto.Schema

  schema "audit_logs" do
    field(:entity, :string)
    field(:action, :string)
    field(:user_id, :integer)
    field(:study_id, :integer)
    field(:inserted_at, :naive_datetime)
  end
end
