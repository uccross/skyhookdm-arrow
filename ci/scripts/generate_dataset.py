import os
import shutil
import random

import pandas as pd

if __name__ == "__main__":
    # generate the test dataframe
    data = {
        "total_amount": list(),
        "fare_amount": list()
    }
    for i in range(0, 500):
        data['total_amount'].append(random.randint(1,11)*5)
        data['fare_amount'].append(random.randint(1,11)*3)
    df = pd.DataFrame(data)

    # dump the dataframe to a parquet file
    df.to_parquet("skyhook_test_data.parquet")

    # create the dataset by copying the parquet files
    shutil.rmtree("nyc", ignore_errors=True)
    payment_type = ["1", "2", "3", "4"]
    vendor_id = ["1", "2"]
    for p in payment_type:
        for v in vendor_id:
            path = f"nyc/payment_type={p}/VendorID={v}"
            os.makedirs(path, exist_ok=True)
            shutil.copyfile("skyhook_test_data.parquet", os.path.join(path, f"{p}.{v}.parquet"))
