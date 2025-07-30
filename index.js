import { processDatabaseOperation } from "./dbOps.js";
import { processRedisOperation } from "./redisOps.js";
import { createRedisClient } from "./redisConfig.js";
import pool from "./mySqlConfig.js";

const redisClient = createRedisClient();
const redisPublisher = createRedisClient();

/**
 * Updates the vehicle cache in Redis with the provided data.
 * Requires org_id, vehicle_id, device_id in data.
 * Validates coordinates and sets data with TTL=300.
 */
const processVehicleCacheUpdate = async (data) => {
  try {
    if (!data.org_id || !data.vehicle_id || !data.device_id) {
      throw new Error("Vehicle cache update requires org_id, vehicle_id, and device_id");
    }

    const isValidCoordinates =
      data.longitude != null &&
      data.latitude != null &&
      data.longitude !== "" &&
      data.latitude !== "" &&
      parseFloat(data.longitude) !== 0 &&
      parseFloat(data.latitude) !== 0 &&
      parseFloat(data.longitude) >= -180 &&
      parseFloat(data.longitude) <= 180 &&
      parseFloat(data.latitude) >= -90 &&
      parseFloat(data.latitude) <= 90;

    if (!isValidCoordinates) {
      throw new Error("Invalid coordinates for vehicle cache update");
    }

    const redisKey = `${data.org_id}:${data.vehicle_id}:${data.device_id}`;

    const redisValue = {
      latitude: data.latitude.toString(),
      longitude: data.longitude.toString(),
      timestamp: data.timestamp || new Date().toISOString(),
      ...(data.Speed && { speed: data.Speed }),
    };

    await redisClient.set(redisKey, JSON.stringify(redisValue), "EX", 300);
    await redisPublisher.publish(redisKey, JSON.stringify(redisValue));


    return {
      success: true,
      message: "Vehicle cache updated with processed data",
      redisKey,
      data: redisValue,
      updatedAt: new Date().toISOString(),
    };
  } catch (error) {
    console.error("Error updating vehicle cache:", error);
    throw error;
  }
};

/**
 * Fetch org_id and vehicle_id from DB when missing, given device_id.
 */
const fetchOrgAndVehicleIds = async (device_id) => {
  const connection = await pool.getConnection();
  try {
    const [rows] = await connection.execute(
      `SELECT organizationId AS org_id, id AS vehicle_id FROM Vehicle WHERE device_id = ? AND isDeleted = FALSE LIMIT 1`,
      [device_id]
    );
    if (rows.length === 0) {
      throw new Error(`No vehicle found for device_id: ${device_id}`);
    }
    // Convert to string for consistency
    return {
      org_id: rows[0].org_id.toString(),
      vehicle_id: rows[0].vehicle_id.toString(),
    };
  } finally {
    connection.release();
  }
};

export const handler = async (event) => {
  console.log("Received event:", JSON.stringify(event, null, 2));
  try {
    // Parse input data
    const data = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
    console.log("Parsed data:", JSON.stringify(data, null, 2));

    if (!data || !data.device_id) {
      throw new Error("Invalid data: device_id is required");
    }

    // Ensure org_id and vehicle_id exist, fetch if missing
    if (!data.org_id || !data.vehicle_id) {
      try {
        const ids = await fetchOrgAndVehicleIds(data.device_id);
        data.org_id = ids.org_id;
        data.vehicle_id = ids.vehicle_id;
      } catch (err) {
        console.warn(`Warning: Unable to determine org_id/vehicle_id for device: ${data.device_id}`, err.message);
        // Depending on your logic, either:
        // throw err; // to stop processing
        // or proceed without vehicle cache update for missing keys
      }
    }

    // Validate coordinates
    const isValidCoordinates =
      data.longitude != null &&
      data.latitude != null &&
      data.longitude !== "" &&
      data.latitude !== "" &&
      parseFloat(data.longitude) !== 0 &&
      parseFloat(data.latitude) !== 0 &&
      parseFloat(data.longitude) >= -180 &&
      parseFloat(data.longitude) <= 180 &&
      parseFloat(data.latitude) >= -90 &&
      parseFloat(data.latitude) <= 90;

    let redisData = data;

    // If invalid or missing coordinates, try to fetch latest valid location from DB
    if (!isValidCoordinates) {
      console.warn(`Invalid or missing coordinates in payload. Attempting DB fallback. lat=${data.latitude}, lon=${data.longitude}`);
      const connection = await pool.getConnection();
      try {
        const [rows] = await connection.execute(
          `SELECT longitude, latitude, device_id, Speed, numberOfSatellites, RawGNSS, Temprature, 
            GyroX, GyroY, GyroZ, acceleroX, acceleroY, acceleroZ, updatedAt
           FROM LocationData 
           WHERE device_id = ? AND latitude != 0 AND longitude != 0 
           ORDER BY updatedAt DESC 
           LIMIT 1`,
          [data.device_id]
        );
        if (rows.length > 0) {
          redisData = {
            ...data,
            device_id: rows[0].device_id,
            longitude: rows[0].longitude.toString(),
            latitude: rows[0].latitude.toString(),
            Speed: rows[0].Speed,
            UsedSatellites: { GPSSatellitesCount: rows[0].numberOfSatellites },
            RawData: rows[0].RawGNSS,
            EnvironmentalData: { Temperature: rows[0].Temprature },
            Gyroscope: {
              xAxis: rows[0].GyroX,
              yAxis: rows[0].GyroY,
              zAxis: rows[0].GyroZ,
            },
            Accelerometer: {
              xAxis: rows[0].acceleroX,
              yAxis: rows[0].acceleroY,
              zAxis: rows[0].acceleroZ,
            },
            timestamp: rows[0].updatedAt ? rows[0].updatedAt.toISOString() : new Date().toISOString(),
          };
          console.log(`Fetched valid data from MySQL fallback for device_id=${data.device_id}`, redisData);
        } else {
          console.warn(`No valid location data found in DB for device_id=${data.device_id}, skipping Redis caching.`);
          redisData = null;
        }
      } catch (err) {
        console.error(`Database error fetching fallback location data for device_id=${data.device_id}:`, err.message);
        redisData = null;
      } finally {
        connection.release();
      }
    }

    // Prepare operations to be run in parallel
    const operations = [processDatabaseOperation(data)]; // Always save incoming data to DB

    let vehicleCachePromise = null;
    if (redisData) {
      operations.push(processRedisOperation(redisData)); // Update main Redis cache with processed/enriched data
      // Also update vehicle cache with the same data
      if (redisData.org_id && redisData.vehicle_id) {
        vehicleCachePromise = processVehicleCacheUpdate(redisData);
      } else {
        console.warn("Skipping vehicle cache update due to missing org_id or vehicle_id in redisData");
      }
    }

    // Execute DB and Redis updates concurrently
    const [dbResult, redisResult] = await Promise.allSettled(operations);

    // Await vehicle cache update separately (optional)
    let vehicleCacheResult = null;
    if (vehicleCachePromise) {
      try {
        vehicleCacheResult = await vehicleCachePromise;
      } catch (err) {
        vehicleCacheResult = { error: err.message };
      }
    }

    // Prepare and return response
    const response = {
      statusCode: 200,
      body: {
        message: "Processing complete",
        database: dbResult.status === "fulfilled" ? dbResult.value : { error: dbResult.reason?.message },
        redis: redisData
          ? redisResult.status === "fulfilled"
            ? redisResult.value
            : { error: redisResult.reason?.message }
          : { message: "Skipped Redis caching due to invalid or missing data" },
        vehicleCache: vehicleCacheResult || { message: "Vehicle cache update skipped or not attempted" },
      },
    };

    console.log("Final response:", response);
    return response;
  } catch (error) {
    console.error("Error in main handler:", error);
    return {
      statusCode: 500,
      body: {
        message: "Error processing data",
        error: error.message,
      },
    };
  }
};
