import os
import time
import shutil
import argparse

from faker import Faker

file_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "fake_files"
)


def create_folder(file_path: str):
    shutil.rmtree(file_path, ignore_errors=True)
    os.mkdir(file_path)


def write_to_file(fake: Faker, file_path: str, file_name: str):
    with open(os.path.join(file_path, file_name), "w") as f:
        f.writelines(fake.texts(nb_texts=fake.random_int(min=0, max=10)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__file__, description="Fake Text File Generator")
    parser.add_argument(
        "-p",
        "--file_path",
        default=os.path.join(
            os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "fake_files"
        ),
        help="File path",
    )
    parser.add_argument(
        "-m",
        "--max_files",
        type=int,
        default=-1,
        help="The amount of time that a record should be delayed.",
    )
    parser.add_argument(
        "-d",
        "--delay_seconds",
        type=float,
        default=0,
        help="The amount of time that a record should be delayed.",
    )

    args = parser.parse_args()

    fake = Faker()
    Faker.seed(1237)

    create_folder(args.file_path)
    current = 0
    while True:
        write_to_file(
            fake,
            args.file_path,
            f"{''.join(fake.random_letters(length=10)).lower()}.txt",
        )
        current += 1
        if current == args.max_files:
            break
        time.sleep(args.delay_seconds)
