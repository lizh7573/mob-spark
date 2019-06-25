import spray.json._
import DefaultJsonProtocol._

/* This object contains general functions for visualizing trajectories
 * and co-trajectories. */
object Visualize {
  /* For representing a point with longitude and latitude as a Json
   * object. Used with Leaflet for showing trajectories on Open Street
   * Map. Notice the order: (longitude, latitude). */
  case class LonLatJson(`type`: String = "MultiPoint",
    coordinates: Array[(Double, Double)])

  object LonLatJsonProtocol extends DefaultJsonProtocol {
    implicit val LonLatDataFormat = jsonFormat2(LonLatJson)
  }

  /* Take an array of Strings in 'GeoJson' format, then insert this into
   * a prebuild html string that contains all the code neccesary to
   * display these features using Leaflet. The resulting html can be
   * showed in a browser.

   * See http://leafletjs.com/examples/geojson.html for a detailed
   * example of using GeoJson with Leaflet. */
  def genLeafletHTML(features: Array[String]): String = {

    val featureArray = features.reduce(_ + "," +  _)
    val accessToken = "pk.eyJ1IjoiZHRnIiwiYSI6ImNpaWF6MGdiNDAwanNtemx6MmIyNXoyOWIifQ.ndbNtExCMXZHKyfNtEN0Vg"
    /* Location of first view of map [latitude, longitude] */
    val location = "[37.77471008393265, -122.40422604391485]"

    val generatedHTML = f"""<!DOCTYPE html>
  <html>
  <head>
  <title>Maps</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/leaflet.css">
  <style>
  #map {width: 600px; height:400px;}
  </style>

  </head>
  <body>
  <div id="map" style="width: 1000px; height: 600px"></div>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/leaflet.js"></script>
  <script type="text/javascript">
  var map = L.map('map').setView($location, 14);

  L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=$accessToken', {
  maxZoom: 18,
  attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, ' +
  '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, ' +
  'Imagery © <a href="http://mapbox.com">Mapbox</a>',
  id: 'mapbox.streets'
  }).addTo(map);

  var features = [$featureArray];

 colors = features.map(function (_) {return rainbow(100, Math.floor(Math.random() * 100)); });

  for (var i = 0; i < features.length; i++) {
      console.log(i);
      L.geoJson(features[i], {
          pointToLayer: function (feature, latlng) {
              return L.circleMarker(latlng, {
                  radius: 4,
                  fillColor: colors[i],
                  color: colors[i],
                  weight: 1,
                  opacity: 1,
                  fillOpacity: 0.8
              });
          }
      }).addTo(map);
  }


  function rainbow(numOfSteps, step) {
  // This function generates vibrant, "evenly spaced" colours (i.e. no clustering). This is ideal for creating easily distinguishable vibrant markers in Google Maps and other apps.
  // Adam Cole, 2011-Sept-14
  // HSV to RBG adapted from: http://mjijackson.com/2008/02/rgb-to-hsl-and-rgb-to-hsv-color-model-conversion-algorithms-in-javascript
  var r, g, b;
  var h = step / numOfSteps;
  var i = ~~(h * 6);
  var f = h * 6 - i;
  var q = 1 - f;
  switch(i %% 6){
  case 0: r = 1; g = f; b = 0; break;
  case 1: r = q; g = 1; b = 0; break;
  case 2: r = 0; g = 1; b = f; break;
  case 3: r = 0; g = q; b = 1; break;
  case 4: r = f; g = 0; b = 1; break;
  case 5: r = 1; g = 0; b = q; break;
  }
  var c = "#" + ("00" + (~ ~(r * 255)).toString(16)).slice(-2) + ("00" + (~ ~(g * 255)).toString(16)).slice(-2) + ("00" + (~ ~(b * 255)).toString(16)).slice(-2);
  return (c);
  }
  </script>


  </body>
  """
    generatedHTML
  }

}
