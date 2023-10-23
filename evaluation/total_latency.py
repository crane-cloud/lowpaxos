# Read timestamps from the file and remove "Start time:" and "End time:" text
with open("run.txt", "r") as file:
    lines = file.readlines()

if len(lines) < 2:
    print("Error: The file must contain at least two timestamps.")
else:
    # Strip whitespace and remove "Start time:" and "End time:" text, then parse as integers (assuming nanoseconds)
    start_time_nanos = int(lines[0].replace("Start time:", "").strip())
    end_time_nanos = int(lines[1].replace("End time:", "").strip())

    # Convert nanoseconds to seconds and calculate the time difference
    time_difference_seconds = (end_time_nanos - start_time_nanos) / 1e9

    print(f"{time_difference_seconds} seconds")
