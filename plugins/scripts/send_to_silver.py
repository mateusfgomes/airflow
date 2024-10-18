import shutil

def send_to_silver():
    output_path = "/tmp/silver/car_accidents_enriched"    
    new_path = 'datalake/silver'
    shutil.move(output_path, new_path)

if __name__ == "__main__":
    send_to_silver()