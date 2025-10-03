# Projects
Open source projects using the Visual Crossing Weather API.

## 🌤️ Looker Studio Community Connector

A Google Apps Script-based Looker Studio Community Connector that brings Visual Crossing weather data into Looker Studio. This connector supports both single location weather data and batch processing for multiple store locations via Google Sheets integration.

### Features
- **Daily & Hourly Weather Data**: Access comprehensive weather metrics including temperature, humidity, precipitation, wind speed, and more
- **Single Location Mode**: Get weather data for a specific location
- **Batch Store Mode**: Process multiple store locations from a Google Sheet
- **Flexible Date Ranges**: Historical and forecast weather data
- **Multiple Units**: Support for metric, imperial, and scientific units
- **Caching**: Built-in response caching to optimize API usage
- **Error Handling**: Comprehensive error handling and debugging

### Quick Start
1. Get your free API key from [Visual Crossing Weather](https://www.visualcrossing.com/account/)
2. Deploy the connector using Google Apps Script
3. Configure in Looker Studio with your API key and settings

**Repository**: [`visualcrossing_connector/`](./visualcrossing_connector/)

**Documentation**: See the [detailed README](./visualcrossing_connector/README.md) for complete setup and usage instructions.

### Test our connector

**Test Deployment ID:** `AKfycbzxluwKtRgJ1Mu7esmy_2kNJAvA8Xltrm28jMjG8urW`  
**Web app URL:** https://script.google.com/macros/s/AKfycbzxluwKtRgJ1Mu7esmy_2kNJAvA8Xltrm28jMjG8urW/dev

#### Steps to test:

1. **Visit the connector:** [https://lookerstudio.google.com/datasources/create?connectorId=AKfycbzxluwKtRgJ1Mu7esmy_2kNJAvA8Xltrm28jMjG8urW](https://lookerstudio.google.com/datasources/create?connectorId=AKfycbzxluwKtRgJ1Mu7esmy_2kNJAvA8Xltrm28jMjG8urW)

2. **Authorize if required:**
   ![Authorization step](/visualcrossing_connector/assets/img/step1.png)

3. **Fill the parameters and click [CONNECT]:**
   ![Configuration step](/visualcrossing_connector/assets/img/step2.png)

4. **Click [Create Report] to create a report:**
   ![Create Report](/visualcrossing_connector/assets/img/step3.png)

5. **Build your weather report:**
   ![Report Creation](/visualcrossing_connector/assets/img/step4.png)

### Production Connector

**Deployment ID:** `AKfycbybtv7dq1gGqfC8y8mbk8-Iul5ZJ_hBAxq59y9UrvB-oKW_Or1rTYlj6wJLtPaSCxUntQ`  
**Data Studio URL:** [https://datastudio.google.com/datasources/create?connectorId=AKfycbybtv7dq1gGqfC8y8mbk8-Iul5ZJ_hBAxq59y9UrvB-oKW_Or1rTYlj6wJLtPaSCxUntQ&authuser=0](https://datastudio.google.com/datasources/create?connectorId=AKfycbybtv7dq1gGqfC8y8mbk8-Iul5ZJ_hBAxq59y9UrvB-oKW_Or1rTYlj6wJLtPaSCxUntQ&authuser=0)  
**Web app URL:** https://script.google.com/macros/s/AKfycbybtv7dq1gGqfC8y8mbk8-Iul5ZJ_hBAxq59y9UrvB-oKW_Or1rTYlj6wJLtPaSCxUntQ/exec