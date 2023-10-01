import argus_api
import argus_reformat
import bigQuery
import schedule
from datetime import datetime


def run():
    # Get new files from Argus
    print("[Server] run argus_api.main()")
    argus_api.main()

    # Reformat files
    print("[Server] run argus_reformat.main()")
    argus_reformat.main()

    # Upload to BigQuery
    print("[Server] run bigQuery.argus()")
    bigQuery.argus()

    # Output Message
    print(f"[Server] completed run for {datetime.now().date()}")


def main():
    schedule.every().day.at("06:00").do(run)
    while True:
        schedule.run_pending()


if __name__ == "__main__":
    run()
    #main()
