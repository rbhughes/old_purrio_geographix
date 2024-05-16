from purr_worker import PurrWorker


if __name__ == "__main__":
    pw = PurrWorker()
    pw.start_queue_processing()
    pw.listen()
