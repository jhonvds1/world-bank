from src.load.load import run_load  # Importa a função que executa a etapa de load do ETL
import logging                       # Para acompanhar o que acontece durante a execução

# Configura o logging para exibir informações no terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
main_logger = logging.getLogger("main")  # Logger específico para a execução do pipeline

def main():
    """
    Função principal que executa o pipeline completo.
    Aqui estamos focando na etapa de load, mas poderia ser expandido para incluir
    extração e transformação, criando um fluxo completo de ETL.
    """
    try:
        main_logger.info("Iniciando o Pipeline")  # Log de início do pipeline
        run_load()                                # Executa a etapa de carga de dados no banco
    except Exception as e:
        # Caso haja algum erro crítico, logamos e interrompemos a execução
        main_logger.info("Falha crítica no Pipeline")
        raise e

# Quando este script é executado diretamente, chamamos a função main
if __name__ == "__main__":
    main()
    main_logger.info("Pipeline finalizado com sucesso")  # Log de término do pipeline