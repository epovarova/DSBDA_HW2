import argparse
import datetime
import os
import random

ACTIONS = [1, 2, 3]


def random_timestamp(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return datetime.datetime.fromtimestamp((start + datetime.timedelta(seconds=random_second)).timestamp())


def main(count, output, users, news, start_date, end_date):
    if not os.path.exists(output):
        os.makedirs(output)

    for id in range(int(count)):
        with open(os.path.join(output, str(id)), "w") as f:
            news_id = random.randrange(1, int(news))
            user_id = random.randrange(1, int(users))
            action = random.choice(ACTIONS)
            timestamp = random_timestamp(start_date, end_date)
            f.write('{},{},{},{},\'{}\'\n'.format(id, news_id, user_id, action, timestamp))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count")
    parser.add_argument("-o", "--output")
    parser.add_argument("-u", "--users")
    parser.add_argument("-n", "--news")
    parser.add_argument("-s", "--start_date", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))
    parser.add_argument("-e", "--end_date", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))
    args = vars(parser.parse_args())

    assert int(args["count"]) > 0, "Count must be positive number"
    assert (args["output"] != None), "Output must be set"
    assert int(args["users"]) > 0, "Users must be positive number"
    assert int(args["news"]) > 0, "News must be positive number"
    assert args["start_date"] < args["end_date"], "End date mast be more that start date"

    main(**args)
