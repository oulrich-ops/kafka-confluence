from pathlib import Path

module_dir = Path(__file__).parent  
file_path = module_dir / "client.properties"

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open(file_path) as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config