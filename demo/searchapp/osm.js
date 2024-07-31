let map_osm;
let markers 		 = [];
let drawnItems 		 = new L.FeatureGroup();
let currentRectangle = null;
let currentCircle    = null;
let drawControl      = null;
let rectangleDrawer  = null;
let drawingEnabled   = false;
let overlayVisible   = false;
let editEnabled      = false;
let altViewVisible   = false;

var redIcon = new L.Icon({
  iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
  iconSize: [20, 34],
  iconAnchor: [6, 20],
  popupAnchor: [1, -34],
  shadowSize: [20, 20],
});

function initmaposm() {
	map_osm = L.map('mapid').setView([39.212156, -99.734376], 4);

	// Add OpenStreetMap tiles to the map
	L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
		maxZoom: 19,
		attribution: '© OpenStreetMap contributors'
	}).addTo(map_osm);

	// Add the FeatureGroup to the map
	map_osm.addLayer(drawnItems);

	// Initialize the draw control and pass it the FeatureGroup of editable layers
	drawControl = new L.Control.Draw({
		edit: {
			featureGroup: drawnItems,
			remove: true
		},
		draw: {
			rectangle: true,
			polyline: false,
			polygon: false,
			circle: true,
			marker: {
				icon: redIcon,
				draggable: true
			},
			circlemarker: false 
		}
	});
	map_osm.addControl(drawControl);

	// Create an instance of L.Draw.Rectangle
	rectangleDrawer = new L.Draw.Rectangle(map_osm, drawControl.options.draw.rectangle);
	circleDrawer    = new L.Draw.Circle(map_osm, drawControl.options.draw.circle);


	// Handle drawing created event
	map_osm.on(L.Draw.Event.CREATED, function (e) {
		let layer = e.layer;
		drawnItems.addLayer(layer);

		// If the created layer is a marker, set its icon to redIcon
		if (e.layerType === 'marker') {
			layer.setIcon(redIcon);
			layer.options.draggable = true;

			// Handle marker drag events
			layer.on('dragend', function(event) {
				let marker = event.target;
				let position = marker.getLatLng();
				marker.setLatLng(
					position, {draggable: true}
				).bindPopup(
					`<b>Coordinates:</b><br>Lat: ${position.lat.toFixed(5)}<br>Lon: ${position.lng.toFixed(5)}`
				).openPopup();
			});
		} else if (e.layerType === 'rectangle') {
			currentRectangle = layer;
			animateRectangle(layer);
		} else if (e.layerType === 'circle') {
			currentCircle = layer;
			animateRectangle(layer);
		}
	});


	// Handle drawing start event
	map_osm.on(L.Draw.Event.DRAWSTART, function () {
		drawnItems.clearLayers();
	});


	document.addEventListener('keydown', function (event) {
		if (
			// input and not in overlay
			(event.target.tagName === 'INPUT')
				|| 
			(event.target.tagName === 'TEXTAREA')
		) {
			if (
				(event.key === 'Enter')
					||
				(event.key === 'Escape')
			) {
				// Exit the text box if the id is not submit-coordinates
				event.target.blur();

				if ((event.target.id !== 'lat') && (event.target.id !== 'lon')) {
					return;
				}
			} else if (event.key === ' ') {
				if (overlayVisible) {
					toggleOverlay();
				}
				return;
			} else {
				return;
			}
		}

		if (event.key === 'r') {
			if (!drawingEnabled) {
				drawingEnabled = true;
				rectangleDrawer.enable();
			} else {
				drawingEnabled = false;
				rectangleDrawer.disable();
			}
		} else if (event.key === 'c') {
			if (!drawingEnabled) {
				drawingEnabled = true;
				circleDrawer.enable();
			} else {
				drawingEnabled = false;
				circleDrawer.disable();
			}
			// control v
		} else if (event.key === 'Escape') {
			if (overlayVisible) {
				toggleOverlay();
			} else {
				drawnItems.clearLayers();
				drawingEnabled = false;
				rectangleDrawer.disable();

				currentRectangle = null;
				currentCircle = null;

				search();
			}
		} else if (event.key === 'e') {
			// Toggle Edit Layers
			if (!editEnabled) {
				drawControl._toolbars.edit._modes.edit.handler.enable();
				editEnabled = true;
			} else {
				drawControl._toolbars.edit._modes.edit.handler.disable();
				editEnabled = false;
			}
		} else if (event.key === ' ') {
			toggleOverlay();
		} else if (event.key === '1') {
			if (altViewVisible) {
				toggleView();
			}
		} else if (event.key === '2') {
			if (!altViewVisible) {
				toggleView();
			}
		} else if (event.key === 'Enter') {
			if (overlayVisible) {
				// If the overlay is visible, call the submit function
				document.getElementById('submit-coordinates').click();
			}

			if (currentRectangle) {
				currentRectangle.setStyle({ color: 'red', dashArray: null });
				removeSpinningEffect(currentRectangle);
			}

			if (currentCircle) {
				currentCircle.setStyle({ color: 'red', dashArray: null });
				removeSpinningEffect(currentCircle);
			}

			if (editEnabled) {
				drawControl._toolbars.edit._modes.edit.handler.disable();
				editEnabled = false;
			}

			search();
		}
	});

	/*
	document.getElementById('paste-coordinates').addEventListener('click', function() {
		navigator.clipboard.readText().then(function(text) {
			// Split the text by comma, space, or tab
			const coordinates = text.split(/[\s,]+/);

			// Ensure we have exactly two values
			if (coordinates.length === 2) {
				document.getElementById('lat').value = coordinates[0];
				document.getElementById('lon').value = coordinates[1];
			} else {
				alert('Please paste valid coordinates in the format "lat, lon" or "lat lon" or "lat\tlon".');
			}
		}).catch(function(err) {
			console.error('Failed to read clipboard contents: ', err);
		});
	});
	*/

	document.getElementById('submit-coordinates').addEventListener('click', function () {
		let lat = document.getElementById('lat').value;
		let lon = document.getElementById('lon').value;

		if (lat === '' || lon === '') {
			return;
		}

		let lat_f = parseFloat(lat);
		let lon_f = parseFloat(lon);

		if (
			(isNaN(lat_f) || isNaN(lon_f))
				||
			(lat_f < -90 || lat_f > 90)
				||
			(lon_f < -180 || lon_f > 180)
			) {
			alert('Invalid Coordinates');
			return;
		}

		placeCircle(lat_f, lon_f);
		toggleOverlay();
	});

	map_osm.on('mousemove', function (e) {
		let latlng = e.latlng;
		let lat = latlng.lat.toFixed(5);
		let lng = latlng.lng.toFixed(5);
		let coordinates = latlng ? `Lat: ${lat}, Lng: ${lng}` : 'No Coordinates';
		let cursorDiv = document.getElementById('cursor-coordinates');
		cursorDiv.innerHTML = coordinates;
		cursorDiv.style.top = (e.originalEvent.clientY - 15) + 'px';
		cursorDiv.style.left = (e.originalEvent.clientX + 10) + 'px';
	});


	// Add custom toggle control
	L.Control.CustomToggle = L.Control.extend({
		onAdd: function(map) {
			var div = L.DomUtil.create('div', 'leaflet-control-custom');
			div.title = 'Toggle View';
			div.onclick = function() {
				toggleView();
			};
			return div;
		}
	});

	L.control.customToggle = function(opts) {
		return new L.Control.CustomToggle(opts);
	}

	L.control.customToggle({ position: 'topright' }).addTo(map_osm);

	document.getElementById('alt-toggle').onclick = function() {
		toggleView();
	};
}

function animateRectangle(layer) {
	let path = layer._path;
	if (path) {
		path.classList.add('spinning-dash');
	}
}

function removeSpinningEffect(layer) {
	let path = layer._path;
	if (path) {
		path.classList.remove('spinning-dash');
	}
}

function toggleOverlay() {
	let overlay = document.getElementById('overlay');
	overlay.style.display = overlayVisible ? 'none' : 'flex';
	overlayVisible = !overlayVisible;

	// Clear the lat and lon input boxes
	document.getElementById('lat').value = '';
	document.getElementById('lon').value = '';
}


function placeCircle(lat, lon) {
	// fire DRAWSTART event to remove any existing rectangles
	map_osm.fire(L.Draw.Event.DRAWSTART);

	currentCircle = L.circle([lat, lon], { radius: 50000, color: "red" }).addTo(map_osm);
	drawnItems.addLayer(currentCircle);

	// simulate Enter key press event to fix the bounding box
	document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter' }));

	map_osm.setView([lat, lon], 5);
}


function toggleView() {
	let mapContainer = document.querySelector('.leaflet-container');
	let altViewDiv = document.getElementById('alt-view');
	if (altViewVisible) {
		mapContainer.style.display = 'block';
		altViewDiv.style.display = 'none';
		document.getElementById('cursor-coordinates').style.display = 'block';
	} else {
		mapContainer.style.display = 'none';
		altViewDiv.style.display = 'block';
		document.getElementById('cursor-coordinates').style.display = 'none';
	}
	altViewVisible = !altViewVisible;
}

function updateMarkers(data) {
	// Clear existing markers
	if (markers.length > 0) {
		markers.forEach(marker => {
			map_osm.removeLayer(marker);
		});
	}
	let idx = 0;

	// Add new markers
	data.results.forEach(item => {
		idx++;

		if (item.lat === null || item.lon === null) {
			return;
		}
		let lat_f = parseFloat(item.lat);
		let lon_f = parseFloat(item.lon);

		if (
			(isNaN(lat_f) || isNaN(lon_f))
				||
			(lat_f < -90 || lat_f > 90)
				||
			(lon_f < -180 || lon_f > 180)
			) {
			return;
		}

		let marker = L.marker([lat_f, lon_f], { icon: redIcon }).addTo(map_osm);
		marker.bindPopup(
			`<b>${item.name}</b><br>${item.address}<br>${item.city} Row: ${idx}`
		);
		markers.push(marker);
	});
}

initmaposm();
