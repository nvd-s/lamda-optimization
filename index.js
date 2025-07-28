import { processDatabaseOperation } from "./dbOps.js";
import { processRedisOperation } from "./redisOps.js";
import pool from "./mySqlConfig.js";

export const handler = async (event) => {
  console.log("Received event:", JSON.stringify(event, null, 2));

  try {
    // 1. Parse data
    const data = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
    console.log("Parsed data:", JSON.stringify(data, null, 2));

    // 2. Validate data
    if (!data || !data.device_id) {
      throw new Error("Invalid data: device_id is required");
    }

    // Validate longitude and latitude (based on insertLocationData logic)
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

    let redisData = data; // Data to publish to Redis

    // 3. If invalid or missing coordinates, fetch from MySQL
    if (!isValidCoordinates) {
      console.warn(`Invalid or missing coordinates: latitude=${data.latitude}, longitude=${data.longitude}`);
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
            device_id: rows[0].device_id,
            longitude: rows[0].longitude.toString(), // Match input format
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
            timestamp: rows[0].updatedAt.toISOString(),
          };
          console.log(`Fetched valid data from MySQL for ${data.device_id}:`, redisData);
        } else {
          console.warn(`No valid data in MySQL for ${data.device_id}, skipping Redis write`);
          redisData = null; // Skip Redis if no valid data
        }
      } catch (dbError) {
        console.error(`MySQL query failed for ${data.device_id}:`, dbError.message);
        redisData = null; // Skip Redis on DB error
      } finally {
        connection.release();
      }
    }

    // 4. Prepare operations
    const operations = [processDatabaseOperation(data)]; // Always write to MySQL
    if (redisData) {
      operations.push(processRedisOperation(redisData)); // Write to Redis if valid or fetched
    }

    // 5. Execute operations
    const [dbResult, redisResult] = await Promise.allSettled(operations);

    // 6. Handle results
    const response = {
      statusCode: 200,
      body: {
        message: "Processing complete",
        database:
          dbResult.status === "fulfilled"
            ? dbResult.value
            : { error: dbResult.reason?.message },
        redis: redisData
          ? redisResult?.status === "fulfilled"
            ? redisResult.value
            : { error: redisResult?.reason?.message }
          : { message: "Skipped Redis due to invalid or missing data" },
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