from data_type_inference import load_data, infer_data_types


def test_inference():
    df = load_data("sample_data.csv")
    df, inferred_types = infer_data_types(df)
    print(inferred_types)


if __name__ == "__main__":
    test_inference()
