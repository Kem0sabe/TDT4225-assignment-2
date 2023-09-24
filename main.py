from Queries import Queries


def main():
    queries = Queries()

    while True:
        print("Select a task to run (1-12) or to run all type 'all' or type 'exit' to close:")
        task = input()

        if task == "exit":
            print("Exiting...")
            break
        if task == "all":
            queries.task_1()
            queries.task_2()
            queries.task_3()
            queries.task_4()
            queries.task_5()
            queries.task_6()
            queries.task_7()
            queries.task_8()
            queries.task_9()
            queries.task_10()
            queries.task_11()
            queries.task_12()
        elif task == "1":
            queries.task_1()
        elif task == "2":
            queries.task_2()
        elif task == "3":
            queries.task_3()
        elif task == "4":
            queries.task_4()
        elif task == "5":
            queries.task_5()
        elif task == "6":
            queries.task_6()
        elif task == "7":
            queries.task_7()
        elif task == "8":
            queries.task_8()
        elif task == "9":
            queries.task_9()
        elif task == "10":
            queries.task_10()
        elif task == "11":
            queries.task_11()
        elif task == "12":
            queries.task_12()
        else:
            print("Invalid task, try again")


if __name__ == "__main__":
    main()
