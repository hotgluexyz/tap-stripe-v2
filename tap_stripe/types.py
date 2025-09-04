from singer_sdk import typing as th

class BalanceTransactionType(th.ObjectType):
    def __init__(self):
        super().__init__(
                            th.Property("id", th.StringType),
                            th.Property("object", th.StringType),
                            th.Property("amount", th.NumberType),
                            th.Property("available_on", th.NumberType),
                            th.Property("created", th.DateTimeType),
                            th.Property("currency", th.StringType),
                            th.Property("description", th.StringType),
                            th.Property("fee", th.NumberType),
                            th.Property("fee_details", th.CustomType({"type": ["array", "string"]})),
                            th.Property("net", th.NumberType),
                            th.Property("reporting_category", th.StringType),
                            th.Property("source", th.StringType),
                            th.Property("status", th.StringType),
                            th.Property("type", th.StringType),
                            th.Property("livemode", th.BooleanType),
                            th.Property("period", th.CustomType({"type": ["object", "string"]})),
                            th.Property("subscription_item", th.StringType),
                            th.Property("total_usage", th.IntegerType),
                            th.Property("exchange_rate", th.NumberType)
                        )