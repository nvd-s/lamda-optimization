import { processDatabaseOperation } from "./dbOps.js";
import { processRedisOperation } from "./redisOps.js";
import { createRedisClient } from "./redisConfig.js";
import pool from "./mySqlConfig.js";

const redisPublisher = createRedisClient();

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

    // Fetch org_id and vehicle_id if missing
    let { org_id, vehicle_id, device_id, latitude, longitude } = data;
    const connection = await pool.getConnection();

    try {
      if (!org_id || !vehicle_id) {
        console.log(`Fetching org_id and vehicle_id for device_id: ${device_id}`);
        const [rows] = await connection.execute(
          `SELECT organizationId, id 
           FROM Vehicle 
           WHERE device_id = ? AND isDeleted = FALSE 
           LIMIT 1`,
          [device_id]
        );

        if (rows.length === 0) {
          throw new Error(`No vehicle found for device_id: ${device_id}`);
        }

        org_id = org_id || rows[0].organizationId.toString();
        vehicle_id = vehicle_id || rows[0].id.toString();
        data.org_id = org_id;
        data.vehicle_id = vehicle_id;
        console.log(`Resolved: org_id=${org_id}, vehicle_id=${vehicle_id}`);
      }

      // Validate coordinates
      const isValidCoordinates =
        latitude != null &&
        longitude != null &&
        latitude !== "" &&
        longitude !== "" &&
        parseFloat(longitude) !== 0 &&
        parseFloat(latitude) !== 0 &&
        parseFloat(longitude) >= -180 &&
        parseFloat(longitude) <= 180 &&
        parseFloat(latitude) >= -90 &&
        parseFloat(latitude) <= 90;

      let redisData = data;
      let vehicleUpdateFailed = false;

      // 3. Update Vehicle table if coordinates are valid
      if (isValidCoordinates) {
        try {
          const [result] = await connection.execute(
            `UPDATE Vehicle 
             SET latitude = ?, longitude = ?, updatedAt = NOW()
             WHERE organizationId = ? AND id = ? AND device_id = ? AND isDeleted = FALSE`,
            [
              parseFloat(latitude),
              parseFloat(longitude),
              parseInt(org_id),
              parseInt(vehicle_id),
              device_id,
            ]
          );
          if (result.affectedRows === 0) {
            console.warn(`No vehicle found to update for ${org_id}:${vehicle_id}:${device_id}`);
            vehicleUpdateFailed = true;
          }
        } catch (error) {
          console.error(`Failed to update Vehicle table for ${org_id}:${vehicle_id}:${device_id}:`, error.message);
          vehicleUpdateFailed = true;
        }
      }

      // 4. If coordinates are invalid or Vehicle update failed, query LocationData
      if (!isValidCoordinates || vehicleUpdateFailed) {
        console.warn(
          `Invalid coordinates or Vehicle update failed: latitude=${latitude}, longitude=${longitude}`
        );
        try {
          const [rows] = await connection.execute(
            `SELECT longitude, latitude, device_id, Speed, numberOfSatellites, RawGNSS, Temprature, 
                    GyroX, GyroY, GyroZ, acceleroX, acceleroY, acceleroZ, updatedAt
             FROM LocationData 
             WHERE device_id = ? AND latitude != 0 AND longitude != 0 
             ORDER BY updatedAt DESC 
             LIMIT 1`,
            [device_id]
          );

          if (rows.length > 0) {
            redisData = {
              org_id,
              vehicle_id,
              device_id: rows[0].device_id,
              latitude: rows[0].latitude.toString(),
              longitude: rows[0].longitude.toString(),
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
            console.log(`Fetched valid data from LocationData for ${device_id}:`, redisData);

            // Update Vehicle table with LocationData coordinates
            try {
              await connection.execute(
                `UPDATE Vehicle 
                 SET latitude = ?, longitude = ?, updatedAt = NOW()
                 WHERE organizationId = ? AND id = ? AND device_id = ? AND isDeleted = FALSE`,
                [
                  parseFloat(rows[0].latitude),
                  parseFloat(rows[0].longitude),
                  parseInt(org_id),
                  parseInt(vehicle_id),
                  device_id,
                ]
              );
            } catch (error) {
              console.error(`Failed to update Vehicle table with LocationData:`, error.message);
            }
          } else {
            console.warn(`No valid data in LocationData for ${device_id}, skipping Redis write`);
            redisData = null;
          }
        } catch (dbError) {
          console.error(`LocationData query failed for ${device_id}:`, dbError.message);
          redisData = null;
        }
      }

      // 5. Prepare operations
      const operations = [processDatabaseOperation(data)];

      if (redisData) {
        const redisKey = `${redisData.org_id}:${redisData.vehicle_id}:${redisData.device_id}`;
        const pubSubChannel = `${redisData.org_id}:${redisData.vehicle_id}:${redisData.device_id}`;
        const redisValue = {
          latitude: redisData.latitude,
          longitude: redisData.longitude,
        };

        operations.push(
          (async () => {
            try {
              const redisResult = await processRedisOperation({
                key: redisKey,
                value: JSON.stringify(redisValue),
                ttl: 300,
              });
              await redisPublisher.publish(pubSubChannel, JSON.stringify(redisValue));
              console.log(`Published to Redis Pub/Sub channel ${pubSubChannel}:`, redisValue);
              return redisResult;
            } catch (redisError) {
              console.error(`Redis operation failed for ${redisKey}:`, redisError.message);
              throw redisError;
            }
          })()
        );
      }

      // 6. Execute operations
      const [dbResult, redisResult] = await Promise.allSettled(operations);

      // 7. Handle results
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
    } finally {
      connection.release();
    }
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