import jmespath
import json

if __name__ == "__main__":
    with open("./return_file_format_drawer_labels.json", mode="r") as f:
        obj = json.load(f)

    all_labels = jmespath.search(
        "DRAWER_DETAIL_LIST[*]",
        obj
    )
    all_values = jmespath.search(
        "DRAWER_DETAIL_LIST[*].VALUE",
        obj
    )
    print(all_labels)
    # print(all_values))