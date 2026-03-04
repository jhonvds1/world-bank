from src.load.load import run_load  # Importa a função que executa a etapa de load do ETL
import logging                       # Para acompanhar o que acontece durante a execução
from src.extract.extract import run_extract
from src.transform.transform import run_transform
from pathlib import Path
import papermill as pm


# Configura o logging para exibir informações no terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
main_logger = logging.getLogger("main")  # Logger específico para a execução do pipeline

def run_pipeline() -> None:
    """
    Função principal que executa o pipeline completo.
    """
    try:
        main_logger.info("Iniciando o Pipeline")  # Log de início do pipeline
        data = run_extract()
        df = run_transform(data)        
        run_load(df)                                # Executa a etapa de carga de dados no banco

        # caminho relativo ao main.py
        logging.info("Executando Notebook")
        notebook_path = Path(__file__).resolve().parent.parent / "notebooks" / "visualizacao.ipynb"

        pm.execute_notebook(
            str(notebook_path),
            str(notebook_path)
)
        logging.info("Notebook executado com Sucesso")


    except Exception as e:
        # Caso haja algum erro crítico, logamos e interrompemos a execução
        main_logger.info("Falha crítica no Pipeline")
        raise e

# Quando este script é executado diretamente, chamamos a função main
if __name__ == "__main__":
    run_pipeline()
    main_logger.info("Pipeline finalizado com sucesso")  # Log de término do pipeline