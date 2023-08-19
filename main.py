import asyncio

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore,
)

BLOB_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=poc02test0123;AccountKey=aWrZY/SY04Q0swItu+zo67s+tKYMGb1kIiWwp5aRmoVDcfkmknzy0X1l5VOUSU2/sbf3vROx8EX4+AStPvihlg==;EndpointSuffix=core.windows.net"
#"DefaultEndpointsProtocol=https;AccountName=poc01readtest01;AccountKey=O+qiU15r88M5geQdmjn2dtvFagYODH2nPblaDmE98YSMH6KUrdVvx01lp7a2f4niuUqbkQJCXHZX+ASt8z10kQ==;EndpointSuffix=core.windows.net"#"BLOB_STORAGE_CONNECTION_STRING"
BLOB_CONTAINER_NAME = "poc01offset" #"BLOB_CONTAINER_NAME"
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://sendreceivepoc2.servicebus.windows.net/;SharedAccessKeyName=receive01;SharedAccessKey=5EXLXKfFQ/Ax622tEHosGUEfQpViZIVgy+AEhCuZHg8="
#"Endpoint=sb://cy-poc-567-2023-auscypit.servicebus.windows.net/;SharedAccessKeyName=PoC-ActivityLog-Consumer;SharedAccessKey=pCZFPy4hTWizgm8tTo4am9arnrMBTbKpl+AEhCy4oBM=;EntityPath=activitylogs"

EVENT_HUB_NAME = "clienttest01" #"EVENT_HUB_NAME"


async def on_event(partition_context, event):
    # Print the event data.
    print(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id
        )
    )

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME
    )

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR,
        consumer_group= "$Default",
        eventhub_name=EVENT_HUB_NAME,
        checkpoint_store=checkpoint_store,
    )
    async with client:
        # Call the receive method. Read from the beginning of the
        # partition (starting_position: "-1")
        await client.receive(on_event=on_event, starting_position="-1")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())