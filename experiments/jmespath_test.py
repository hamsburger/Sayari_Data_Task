import jmespath
import json

if __name__ == "__main__":
    # with open("./return_file_format_drawer_labels.json", mode="r") as f:
    #     obj = json.load(f)

    # all_labels = jmespath.search(
    #     "DRAWER_DETAIL_LIST[*]",
    #     obj
    # )
    # all_values = jmespath.search(
    #     "DRAWER_DETAIL_LIST[*].VALUE",
    #     obj
    # )
    # print(all_labels)
    # print(all_values))
    array = []
    with open("./graph_example.jsonl", mode="r") as f:
        for line in f:
            array.append(json.loads(line))
    
    from_node = jmespath.search(
        "[*][0]",
        array
    )
    to_node = jmespath.search(
        "[*][1]",
        array
    )
    relationship_type = jmespath.search(
        "[*][2].label",
        array
    )

    print(list(zip(from_node, to_node, relationship_type)))