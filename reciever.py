
# from redis_resc import redis_conn, redis_queue
import json
import heapq as hq
from collections import defaultdict


job_schedular = []
map_priorities_to_jobs = defaultdict(list)
job_id_to_dependency = defaultdict()
job_id_to_json_mapper = defaultdict()
deleted_ids_from_queue = set()


def popd():
    # deleted_ids_from_queue = set()
    # print()
    # print("popping...")
    ordered_job_ids = []
    while job_schedular:
        priority = hq.heappop(job_schedular)

        # print(priority, " priorityyy")
        job_ids = map_priorities_to_jobs[priority]

        while job_ids:

            # print("job_ids: ", job_ids)
            job_id = job_ids.pop()

            if job_id in deleted_ids_from_queue:
                continue
            # print("job_id:  ", job_id)
            # print("job_id_to_dependency: ", job_id_to_dependency)
            dependent = job_id_to_dependency[job_id]
            depend = []
            depend.append(job_id)

            # print("dependent:", dependent)

            while dependent:
                # print()
                # print("depend: ", depend)
                # print("dependent called")
                last_added_item_job_id = depend[-1]
                # print("last_added_item_job_id: ", last_added_item_job_id)
                last_item_dependency_id = job_id_to_dependency[last_added_item_job_id]
                if last_item_dependency_id in deleted_ids_from_queue:
                    break
                # print("last_item_dependency_id: ", last_item_dependency_id)

                if last_item_dependency_id:
                    depend.append(last_item_dependency_id)
                else:
                    dependent = False

            while depend:
                to_remove = depend.pop()
                ordered_job_ids.append(to_remove)
                job = map_priorities_to_jobs[to_remove]

                deleted_ids_from_queue.add(to_remove)
                del map_priorities_to_jobs[to_remove]
                del job_id_to_dependency[to_remove]

    return ordered_job_ids if ordered_job_ids else "Queue Empty!!"


# schedule the data and push it to redis
def redis_push(data):
    import requests
    from jobs import Job
    new_person = Job(**data)
    new_person.save()

# schedule


def schedule(data):
    # print("recieved")
    json_data = json.loads(data)
    hq.heappush(job_schedular, json_data["priority"])

    global map_priorities_to_jobs
    global job_id_to_dependency
    global deleted_ids_from_queue
    global job_id_to_json_mapper

    map_priorities_to_jobs[json_data["priority"]].append(json_data["job_id"])
    job_id_to_dependency[json_data['job_id']] = json_data["dependency"]
    job_id_to_json_mapper[json_data['job_id']] = json_data

    print("job_schedular: ", job_schedular, type(job_schedular))
    print("map_priorities_to_jobs: ", map_priorities_to_jobs)
    print("job_id_to_dependency: ", job_id_to_dependency)

    ordered_job_ids = popd()

    for job_id in ordered_job_ids:
        redis_push(job_id_to_json_mapper[job_id])

    map_priorities_to_jobs = defaultdict(list)
    job_id_to_dependency = defaultdict()
    job_id_to_json_mapper = defaultdict()
    deleted_ids_from_queue = set()

    # print("map_priorities_to_jobs: f", map_priorities_to_jobs)
    # print("job_id_to_dependency: f", job_id_to_dependency)

    # add ordered_job_ids to redis database
