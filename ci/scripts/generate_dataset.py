import os
import shutil

if __name__ == "__main__":
    shutil.rmtree("nyc")
    payment_type = ["1", "2", "3", "4"]
    vendor_id = ["1", "2"]
    for p in payment_type:
        for v in vendor_id:
            path = f"nyc/payment_type={p}/VendorID={v}"
            os.makedirs(path)
            shutil.copyfile("skyhook_test_data.parquet", path)
