from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    item: str = Field(min_length=1, max_length=120)
    quantity: int = Field(default=1, ge=1, le=100, strict=True)
    id: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    simulate_failures: int = Field(default=0, ge=0, le=100, strict=True)

    @field_validator("item")
    @classmethod
    def item_is_not_blank(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("item must not be blank")
        return value
