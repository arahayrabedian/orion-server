import argparse

from jsonslicer import JsonSlicer

from orion.app import create_app
from orion.models.location import Location

"""
Streams the "Location History.json" file and converts each entry under
"locations" in to an orion Location entry and saves to database.
"""


def import_google_location_history_json(
        path_to_json,
        user_to_use,
        device_to_use="google_location_history"
):
    assert path_to_json is not None
    assert user_to_use is not None
    # do all the other work for us, chunky, but includes db.
    app = create_app()

    with open(path_to_json) as location_history_file:
        count = 0
        for location in JsonSlicer(location_history_file, ('locations', None)):
            # {
            #     'timestampMs': '1382007300573',
            #     'latitudeE7': 338876807,
            #     'longitudeE7': 355526483,
            #     'accuracy': 1817
            # }
            timestamp = int(location['timestampMs']) / 1e3  # millis to seconds for owntracks format
            latitude = location['latitudeE7'] / 1e7  # apparently google doesn't like floats.
            longitude = location['longitudeE7'] / 1e7
            accuracy = int(location['accuracy'])
            # print(f"{timestamp}: {latitude},{longitude} {accuracy}")
            count += 1
            parsed_location = Location(
                timestamp=timestamp,
                accuracy=accuracy,
                longitude=longitude,
                latitude=latitude,
                user=user_to_use,
                device=device_to_use,
                battery=None,
                trigger=None,
                connection=None,
                tracker_id=None,
                address=None,
            )
            app.ctx.db.session.add(parsed_location)
            if count % 2500 == 0:
                app.ctx.db.session.commit()
        app.ctx.db.session.commit()  # commit any stragglers.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import Google Takeout to Orion.')
    parser.add_argument('path_to_json',
                        type=str,
                        help='path to the google takeout location history json '
                             '(the one complete file)')
    parser.add_argument('user_to_use',
                        type=str,
                        help='The orion user under which to insert these entries')
    parser.add_argument('--device-to-use',
                        type=str,
                        help="Name of the device to insert these entries under",
                        default="google_location_history")

    args = parser.parse_args()
    import_google_location_history_json(
        args.path_to_json,
        args.user_to_use,
        device_to_use=args.device_to_use
    )
