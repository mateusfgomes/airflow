import shutil

def send_to_bronze():
    output_path = "/tmp/bronze/car_accidents"
    new_path = 'datalake/bronze'
    shutil.move(output_path, new_path)

if __name__ == "__main__":
    send_to_bronze()

