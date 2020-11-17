import src
import dotenv
import os
import argparse

parser = argparse.ArgumentParser(description='Partaker fuser', usage='%(prog)s [options]')
parser.add_argument("--environment","-e", help="Name of the env file", default="dev.env")
args = parser.parse_args()

dotenv.load_dotenv(args.environment)

conn = src.create_connection(
    user=os.environ.get("user"),
    password=os.environ.get("password"),
    host=os.environ.get("host"),
    port=os.environ.get("port"),
    database=os.environ.get("database")
)

duplicated_partakers_dict = src.get_duplicated_partakers(conn)
print("Available partakers to fuse:")
for i,partaker in enumerate(duplicated_partakers_dict.keys()):
    print(f"{i}\t{partaker}")

pending_selection = True
while pending_selection:
    user_input = input(f"Enter the number of the partaker to fuse (0 - {len(duplicated_partakers_dict)-1}): ")
    try:
        if (int(user_input) < len(duplicated_partakers_dict)) & (int(user_input) >= 0):
            pending_selection = False
            user_input = int(user_input)
        else:
            print("Enter a valid partaker number.")
    except:
        print("Enter a valid partaker number.")

partaker_caption = list(duplicated_partakers_dict.keys())[user_input]

print(f"Partaker {partaker_caption} is beign fused.")

src.fuse_partakers(
    conn,
    duplicated_partakers_dict[partaker_caption],
    partaker_caption.upper()
)

print(f"Partaker {partaker_caption} was successfully fused.")