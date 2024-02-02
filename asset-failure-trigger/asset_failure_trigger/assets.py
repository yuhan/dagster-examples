from dagster import asset


@asset
def asset1():
    print("im a happy asset that will succeed")


@asset(deps=[asset1])
def asset_to_fail():
    raise Exception("This asset is designed to fail")


@asset(deps=[asset_to_fail])
def asset2():
    print("i won't run because my dependency failed")
