import { createRedisClient } from "./redisConfig.js";

const MAX_RETRIES = 10;

export const  processRedisOperation = async(data) =>{
  const deviceId = data.device_id;
  const message = JSON.stringify(data);
  let publisher;

  try {
    // Create Redis client
    publisher = await createRedisClient();
    console.log(`Created Redis publisher for device: ${deviceId}`);

    // Publish data with retries
    let retries = 0;

    while (retries < MAX_RETRIES) {
      try {
        await publisher.publish(deviceId, message);
        console.log(`Published to ${deviceId} successfully`);

        return {
          success: true,
          message: "Data published successfully",
          channel: deviceId,
        };
      } catch (error) {
        retries++;
        console.log(
          `Publish attempt ${retries}/${MAX_RETRIES} failed:`,
          error.message
        );

        if (retries >= MAX_RETRIES) {
          throw error;
        }

        // backoff
        await new Promise((resolve) => setTimeout(resolve, 1000 * retries));
      }
    }
  } catch (error) {
    console.error(`Redis operation failed for device ${deviceId}:`, error);
    throw error;
  } finally {
    // Clean up Redis connection
    if (publisher && typeof publisher.quit === "function") {
      try {
        await publisher.quit();
        console.log(`Closed Redis publisher for device ${deviceId}`);
      } catch (err) {
        console.error(`Error closing Redis publisher:`, err);
      }
    }
  }
}

