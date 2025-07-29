import mysql from "mysql2/promise";

export const dbConfig = {
  uri:
    process.env.DATABASE_URL ||
    // "mysql://avnadmin:AVNS_Ee5wJn2FEslVQtUFzR9@mysql-11691c1c-abdul123arj-9223.a.aivencloud.com:13747/defaultdb?ssl-mode=REQUIRED",
    "mysql://root:N@veed123@localhost:3306/trackingapp"
};



const pool = mysql.createPool(dbConfig.uri);
export async function insertLocationData(data) {
  if (
    data.longitude === "" ||
    data.latitude === "" ||
    parseFloat(data.longitude) === parseFloat(0) ||
    parseFloat(data.latitude) === parseFloat(0)
  ) {
    throw new Error("Invalid GPS data: coordinates missing or zero");
  }

  const connection = await pool.getConnection();

  try {
    console.log("data....",data);
    const [result] = await connection.execute(
      `INSERT INTO LocationData 
      (longitude, latitude, device_id, Speed, numberOfSatellites, RawGNSS, Temprature, 
      GyroX, GyroY, GyroZ, acceleroX, acceleroY, acceleroZ, updatedAt) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`,
        [
        parseFloat(data.longitude),
        parseFloat(data.latitude),
        data.device_id,
        data.speed +""?? 0,
        0,
        "",
        0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
      ]
    );

    return result;
  } finally {
    connection.release();
  }
}

export default pool;
