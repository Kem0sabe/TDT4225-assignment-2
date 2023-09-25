from Queries import Queries

def main():
    queries = Queries()

    task_mapping = {
        '1': queries.task_1,
        '2': queries.task_2,
        '3': queries.task_3,
        '4': queries.task_4,
        '5': queries.task_5,
        '6': queries.task_6,
        '7': queries.task_7,
        '8': queries.task_8,
        '9': queries.task_9,
        '10': queries.task_10,
        '11': queries.task_11,
        '12': queries.task_12
    }

    while True:
        print("Select a task to run (1-12), type 'all' to run all, or 'exit' to close:")
        task = input().strip()

        if task == "exit":
            print("Exiting...")
            break
        elif task == "all":
            print("\nRunning all tasks, this might take a while...")
            for func in task_mapping.values():
                func()
        elif task in task_mapping:
            task_mapping[task]()
        else:
            print("Invalid task, try again")

if __name__ == "__main__":
    main()
