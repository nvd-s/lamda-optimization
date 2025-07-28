import { insertLocationData } from "./mySqlConfig.js";


export const  processDatabaseOperation = async(data) =>{
  try {
    // validate data
    if (
      data.longitude === "" ||
      data.latitude === "" ||
      parseFloat(data.longitude) === parseFloat(0) ||
      parseFloat(data.latitude) === parseFloat(0)
    ) {
      return {
        success: false,
        message: "GPS data invalid or unavailable",
      };
    }

    // push the data to DB
    const result = await insertLocationData(data);

    console.log("Database operation complete:", result);
    return {
      success: true,
      message: "Data saved to database successfully",
      insertId: result.insertId,
    };
  } catch (error) {
    console.error("Database operation failed:", error);
    throw error;
  }
}
