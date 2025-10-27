from prefect import flow
from tasks.extract import extract
from tasks.transform import transform
from tasks.load import load


@flow
def main():
    data = extract()
    data_transform = transform(data)
    print(f'transformados {len(data_transform)} registros')
    resultado = load(data_transform)
    print(f'se insertaron {resultado} registros en la bd')
    
if __name__ == "__main__":
    main()