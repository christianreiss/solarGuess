Solar‑generation forecast calculations (product‑agnostic)
Solar‑production forecast services typically combine long‑term solar radiation statistics with short‑term weather forecasts to estimate how much electricity a photovoltaic (PV) system will generate. The calculations involve physics (solar geometry), meteorology (cloud cover and temperature), system characteristics (module power and orientation) and sometimes user‑defined adjustments. The following sections describe the main calculations and assumptions behind modern PV forecasts.
1. Radiation data and clear‑sky models
1.1 Baseline radiation on a tilted plane
A core component of any PV forecast is a database of solar radiation measurements or simulations. The service examined here uses a geographic solar‑radiation database produced with a solar irradiance model (often called r.sun) and published by a European research center. The model computes direct, diffuse and reflected radiation for any location, day and surface orientation[1]. It can incorporate topography by simulating shadows from the horizon[1]. These calculations yield the clear‑sky irradiance – the sunlight at the surface with no clouds – and can be integrated over the day to provide daily irradiation[2].
The model uses input parameters:
    • Latitude and longitude of the site and a declination (tilt) and azimuth describing the plane of the PV modules. Solar‑forecast APIs require these parameters and validate them to ensure they fall within normal ranges[3].
    • Time step and day of year. The r.sun algorithm can compute radiation at fine temporal resolution (e.g., 30‑minute steps) and then integrate over a day[4].
    • Atmospheric turbidity (Linke factor) and albedo describing atmospheric clarity and ground reflectance[5]. Default values are used when site‑specific data are unavailable.
The result is a climatological dataset of daily global irradiation on a tilted plane. These clear‑sky values form the baseline from which actual production is derived. When no clouds are considered, the production forecast represents the theoretical maximum for the site[6].
1.2 Historic averages
For a long‑term reference, many services compute a historic solar production by averaging clear‑sky irradiation over all available years. This yields the typical energy expected on a given day without considering current weather[7]. Such averages are useful for planners but are not real‑time forecasts.
2. Incorporating weather forecasts
2.1 Weather data sources
To convert clear‑sky radiation into expected production, modern services pull weather forecasts from multiple providers. The documentation notes that forecasts combine historic irradiation data with weather forecast data and use cloud coverage and temperature predictions[8]. For each location, the service fetches the predicted cloud cover and ambient temperature in 15‑minute increments, aligning them with the radiation database’s time steps.
2.2 Cloud‑cover reduction factor
The effect of clouds is represented by a clear‑sky factor (also called sky‑clearness index) that scales the clear‑sky irradiance. A simple empirical relationship is often used where the plane‑of‑array irradiance G_actual is approximated as
$$     G_{\text{actual}} = G_{\text{clear\,sky}} \times f(\text{cloud coverage})     $$
The function f decreases from 1 under clear sky to near zero under overcast conditions. Various empirical formulas exist; one example uses f = 1 - 0.75 \times C^{\,3.4}, where C is fractional cloud cover. Forecast services calibrate f by comparing cloud‑cover data with ground measurements. Because the forecast examined here relies on multiple weather providers, the algorithm combines forecasts to produce a weighted average of sky‑clearness indices. The result is a set of irradiance curves for upcoming days at 15‑minute resolution.
2.3 Temperature and module performance
PV modules lose efficiency as cell temperature increases. The electrical power P of a module with nominal peak power P_\text{STC} (measured at standard test conditions of 25 °C and 1 kW/m²) is given approximately by

where G_actual is the actual irradiance (from the clear‑sky database scaled by cloud‑cover factor), G_STC is 1 kW/m², T_cell is the predicted module temperature and γ is the module’s temperature coefficient (typically –0.4 % per °C). To estimate T_cell, empirical models (e.g., those based on nominal operating cell temperature) convert ambient temperature and irradiance into cell temperature. Temperature forecasts from weather providers supply the ambient term for this calculation.
2.4 Combining plane orientations and system power
The forecast service allows users to specify up to four PV “planes” (arrays) with different declinations, azimuths and power ratings. For each plane, the algorithm computes an irradiance time series using the steps above and multiplies by the installed power in kW to obtain a power time series. When multiple planes are supplied, the power series are summed to produce a combined forecast[9].
The service’s responses include several time‑resolved quantities: average power in watts for each period, energy in watt‑hours per period, cumulative energy for the day and daily energy sums[10]. Users may request only one of these by using specialized endpoints[11].
3. User‑adjustable factors
3.1 Damping factors (morning/evening shading)
Many PV systems experience shading in the early morning or late evening, for example from nearby trees or buildings. The forecast service introduces a damping factor that attenuates the forecast during those periods. Users can specify a single parameter damping with one or two values (morning and evening), or separate parameters damping_morning and damping_evening[12]. A damping of 1 leaves the forecast unchanged, whereas lower values proportionally reduce the early or late‑day power. Separate parameters allow API developers to validate inputs; if both morning and evening factors are absent, the service applies no modification[12].
3.2 User‑defined horizon
Shading from surrounding terrain or obstacles can significantly reduce sunlight at low solar elevations. Users can provide a custom horizon profile as a comma‑separated list of horizon heights at equal azimuth intervals. Each value represents the elevation angle (in degrees) of the local horizon at a given compass bearing, starting at north and moving clockwise[13]. At least 12 values are recommended to provide sufficient resolution[13]. A horizon value limits the direct component of solar irradiance for sun positions below the horizon angle; the diffuse component remains nearly unchanged[14]. This feature allows the clear‑sky calculation to include local shading.
3.3 Adjusting forecasts with real production data
Forecast accuracy can be improved by comparing predicted production with actual energy output. The service accepts an actual parameter (in kWh) representing today’s measured energy. When supplied, the algorithm compares this value with its own cumulative energy prediction and scales the remaining forecast for the day accordingly. Documentation notes that the correction affects only the current day and can be reset by sending zero; the optional limit=0 suppresses the response and only applies the correction[15].
3.4 Selecting time windows for controllable loads
For users with controllable loads—such as dishwashers, electric‑vehicle chargers or heaters—the forecast service can suggest time windows when excess solar power is available. Users specify the base load that must be exceeded, a minimum duration (in minutes) and optionally the watt‑hours required. The algorithm calculates the earliest, best and latest windows of the day where the forecasted PV power exceeds the base load and, if specified, provides the required energy[16]. If no minimum duration is given, all intervals with power above the base load (minimum 15 minutes) are returned and only rest‑of‑day windows are provided for the current day[17].
4. Output formats and resolution
Forecast results are delivered in JSON or CSV formats. By default, the service provides data at 15‑minute intervals; this resolution matches the temporal resolution of weather forecasts. Users can request only daily totals or period‑averaged values if desired[11]. The result section of the response includes:
    • watts: average power during each period[18].
    • watt_hours_period: energy produced during each period[18].
    • watt_hours: cumulative energy since midnight[19].
    • watt_hours_day: total energy produced each day[20].
The forecast API also provides endpoints for checking the validity of the input location and plane parameters, returning human‑readable location information and time‑zone data[21]. Invalid inputs generate a HTTP 400 error with an explanatory message[22].
5. Summary of calculation workflow
    1. Validate location and orientation. Ensure that latitude, longitude, tilt and azimuth are within allowed ranges[3] and optionally include a custom horizon[13].
    2. Extract clear‑sky radiation. Use a pre‑computed clear‑sky radiation database (based on a solar‑irradiance model like r.sun) to obtain global irradiation on the specified plane[1].
    3. Fetch weather forecasts. Acquire cloud‑cover and temperature forecasts from multiple providers[8].
    4. Compute actual irradiance. Reduce clear‑sky irradiance by a cloud‑cover factor, incorporate shading (horizon and damping) and adjust for module temperature. Multiply by installed module power to obtain power time series.
    5. Aggregate multiple planes. If the system has several plane orientations, repeat the calculation for each and sum the results[9].
    6. Apply user adjustments. Apply damping factors for morning/evening shading[12] and scale the forecast using actual production data when provided[15].
    7. Compute derived metrics. Calculate watt‑hours per period, cumulative day totals and daily sums[20], and optionally identify time windows when power exceeds a specified base load[16].
    8. Deliver results. Return data in the requested format (JSON or CSV) with 15‑minute resolution and include metadata (location, time zone, rate limit) in the response[23].
Conclusion
Modern PV production forecasts fuse climatological solar‑radiation models with near‑term weather forecasts and incorporate system‑specific parameters and user adjustments. A clear‑sky irradiance model such as r.sun provides baseline radiation on tilted planes[1]. Real‑time cloud‑cover and temperature forecasts are then used to scale that baseline down to realistic irradiance levels. Module power, tilt, azimuth and custom horizon profiles tailor the forecast to a specific installation, while damping factors and actual production data allow users to fine‑tune predictions. These calculations yield power and energy time series that can be used for system monitoring, energy management or scheduling controllable loads.

[1] [2] [4] [5] r.sun - GRASS GIS manual
https://grass.osgeo.org/grass82/manuals/r.sun.html
[3] [6] [7] [9] [10] [11] [18] [19] [20] [23] Solar production estimate [Forecast.Solar]
https://doc.forecast.solar/doku.php
[8] Homepage [Forecast.Solar]
https://forecast.solar/
[12] Damping factor [Forecast.Solar]
https://doc.forecast.solar/damping
[13] [14] User-defined horizon [Forecast.Solar]
https://doc.forecast.solar/horizon
[15] Adjust forecast [Forecast.Solar]
https://doc.forecast.solar/actual
[16] [17] Time windows for controllable loads [Forecast.Solar]
https://doc.forecast.solar/api:timewindows
[21] [22] Misc [Forecast.Solar]
https://doc.forecast.solar/api:misc
