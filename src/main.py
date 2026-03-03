from src.load.load import run_load
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

main_logger = logging.getLogger("main")

def main():
    try:
        main_logger.info("Iniciando o Pipeline")
        run_load()
    except:
        main_logger.info("Falha critica no Pipeline")
        raise

if __name__ == "__main__":
    main()
    main_logger.info("Pipeline finalizado com sucesso")