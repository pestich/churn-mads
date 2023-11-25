
from src import Model, FeatureBuilder
import pandas as pd
import click

model = Model()
builder = FeatureBuilder()

@click.command()
@click.option('--path', prompt='Your name', help='The person to greet.')
def main(path):
    try:
        df = pd.read_parquet(path)
    except:
        return f'Ошибка. Расширение файла должно быть .gzip'
    
    df = builder.prepare_data(df)
    result = model.predict(df)
    result.to_csv('predictions.csv', sep=';', index=False)

if __name__ == '__main__':
    main()
