from dagster import AssetsDefinition, Output, asset


def transform_asset_factory(
    asset_name: str, group_name: str, exchange_id: str
) -> AssetsDefinition:
    """
    Factory function to build a dagster asset, based on arguments
    """

    @asset(name="{asset_name}", group_name="{group_name}")
    def build_asset():

        raw = {}

        """
            build_asset returns an asset
        """
        return Output(value=raw)

    return build_asset
