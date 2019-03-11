from etherfx_worker_daemon import *

def main():
  app = DaemonApp(logging_enabled=True)
  app.run()

if __name__ == "__main__":
  main()