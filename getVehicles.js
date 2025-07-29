import { createRedisClient } from "./redisConfig.js";
import pool from "./mySqlConfig.js";

// Create two Redis clients: one for GET/SET, one for pub/sub
const redisClient = createRedisClient();
const redisPublisher = createRedisClient();

/**
 * Lambda entry handler for vehicle data retrieval.
 * Supports two modes:
 * 1. Fetch one specific vehicle (orgid, vehicleid, deviceid given)
 * 2. Fetch all vehicles for an org (only orgid given)
 */
export const handler = async (event) => {
  console.log("Received event:", JSON.stringify(event, null, 2));
  // Get a mysql connection from the pool
  const connection = await pool.getConnection();

  try {
    // Extract query parameters from the event
    const queryParams = event.queryStringParameters || {};
    const orgId = queryParams.orgid;
    const vehicleId = queryParams.vehicleid;
    const deviceId = queryParams.deviceid;

    let vehicles = [];

    /**
     * CASE 1: Fetch a specific vehicle's data if vehicleId AND deviceId provided
     * Redis is checked first for the key.
     *   - If found: parse and append to response.
     *   - Else: Fetch from DB, cache in Redis with TTL, publish location, add to response.
     */
    if (vehicleId && deviceId) {
      const redisKey = `${orgId}:${vehicleId}:${deviceId}`;
      const redisValue = await redisClient.get(redisKey);

      if (redisValue) {
        // Vehicle found in Redis cache
        vehicles.push({ key: redisKey, ...JSON.parse(redisValue) });
      } else {
        // Not in Redis, get from DB
        const [rows] = await connection.execute(
          `SELECT id, device_id, organizationId, latitude, longitude, VehicleNumber, Make, Model, Year, updatedAt
           FROM Vehicle 
           WHERE organizationId = ? AND id = ? AND device_id = ? AND isDeleted = FALSE 
             AND latitude != 0 AND longitude != 0`,
          [parseInt(orgId), parseInt(vehicleId), deviceId]
        );

        if (rows.length > 0) {
          // Prepare value for Redis cache & publish
          const redisValue = {
            latitude: rows[0].latitude.toString(),
            longitude: rows[0].longitude.toString(),
          };
          // Cache in Redis with 5 minute TTL and publish to channel
          await redisClient.set(redisKey, JSON.stringify(redisValue), "EX", 300);
          await redisPublisher.publish(redisKey, JSON.stringify(redisValue));
          // Construct response object
          vehicles.push({
            key: redisKey,
            ...redisValue,
            vehicle_id: rows[0].id.toString(),
            device_id: rows[0].device_id,
            org_id: rows[0].organizationId.toString(),
            vehicle_number: rows[0].VehicleNumber,
            make: rows[0].Make,
            model: rows[0].Model,
            year: rows[0].Year,
            timestamp: rows[0].updatedAt.toISOString(),
          });
        }
      }
    } else {
      /**
       * CASE 2: Fetch all vehicles for this org
       * 1. Try to get all cached locations from Redis first (using SCAN for pattern orgid:*:*)
       * 2. If not found, get all active vehicles with locations from DB, set and publish to Redis, build array.
       */
      const pattern = `${orgId}:*:*`; // Pattern for Redis keys for this org
      const keys = [];
      let cursor = 0;

      // SCAN for all keys for this org
      do {
        const [nextCursor, scanKeys] = await redisClient.scan(cursor, "MATCH", pattern, "COUNT", 100);
        cursor = parseInt(nextCursor);
        keys.push(...scanKeys);
      } while (cursor !== 0);

      if (keys.length > 0) {
        // If keys found in Redis, use MGET for efficiency
        const values = await redisClient.mget(keys);
        vehicles = values.map((value, index) => ({
          key: keys[index],
          ...(value ? JSON.parse(value) : {}),
        }));
      } else {
        // Not found in Redis: fetch all vehicles with valid location from DB
        const [rows] = await connection.execute(
          `SELECT id, device_id, organizationId, latitude, longitude, VehicleNumber, Make, Model, Year, updatedAt
           FROM Vehicle 
           WHERE organizationId = ? AND isDeleted = FALSE 
             AND latitude != 0 AND longitude != 0`,
          [parseInt(orgId)]
        );

        // For each vehicle row, cache location in Redis and publish update
        for (const row of rows) {
          const redisKey = `${row.organizationId}:${row.id}:${row.device_id}`;
          const redisValue = {
            latitude: row.latitude.toString(),
            longitude: row.longitude.toString(),
          };
          await redisClient.set(redisKey, JSON.stringify(redisValue), "EX", 300);
          await redisPublisher.publish(redisKey, JSON.stringify(redisValue));
          vehicles.push({
            key: redisKey,
            ...redisValue,
            vehicle_id: row.id.toString(),
            device_id: row.device_id,
            org_id: row.organizationId.toString(),
            vehicle_number: row.VehicleNumber,
            make: row.Make,
            model: row.Model,
            year: row.Year,
            timestamp: row.updatedAt.toISOString(),
          });
        }
      }
    }

    // Return successful response with all assembled vehicles
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Vehicle data retrieved",
        vehicles,
      }),
    };

  } catch (error) {
    // Log and return error response
    console.error("Error fetching vehicle data:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Error fetching vehicle data",
        error: error.message,
      }),
    };
  } finally {
    // Release the DB connection back to the pool
    connection.release();
  }
};
