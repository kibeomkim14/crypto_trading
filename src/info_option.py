import json
import requests
import pandas as pd

from urls import BASE_OPTION_INFO_URL
from paths import BASE_DATA_PATH


if __name__ == "__main__":
    # all tradable option contract info
    option_info_dict = json.loads(requests.get(BASE_OPTION_INFO_URL).text)
    underlyings_info = option_info_dict["optionContracts"]
    underlyings_info = pd.DataFrame(underlyings_info)
    all_contract_info = option_info_dict["optionSymbols"]

    option_contract_info = []
    for i in range(len(all_contract_info)):
        contract = option_info_dict["optionSymbols"][i]
        _ = contract.pop("filters")
        option_contract_info.append(contract)

    option_contract_info = pd.DataFrame(option_contract_info)
    trade_info = option_contract_info.loc[
        :, ["id", "underlying", "priceScale", "quantityScale", "quoteAsset"]
    ]
    option_contract_info = option_contract_info.loc[
        :, ["id", "symbol", "underlying", "side", "strikePrice", "expiryDate", "unit"]
    ]
    option_contract_info["expiryDate"] = pd.to_datetime(
        option_contract_info["expiryDate"], unit="ms"
    )

    # save option exchange information as csv
    underlyings_info.to_csv(BASE_DATA_PATH + "underlyings.csv")
    option_contract_info.to_csv(BASE_DATA_PATH + "contracts.csv")
