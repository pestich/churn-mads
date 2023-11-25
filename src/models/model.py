import pickle as pk
import pandas as pd



class Model:
    def __init__(self) -> None:
        with open(f'./models/cat_classifier2.pkl', "rb") as f:
            model1 = pk.load(f)
        self.classifier = model1

        with open(f"./models/cat_regression2.pkl", "rb") as f:
            model2 = pk.load(f)
        self.regressor = model2


    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        data_submit = df
        predicted_submit = self.classifier.predict(df)
        data_submit['buy_post'] = predicted_submit
        submit_predict = self.regressor.predict(df)
        data_submit['date_diff_post'] = submit_predict
        result = data_submit[['customer_id', 'date_diff_post', 'buy_post']]
        return result
