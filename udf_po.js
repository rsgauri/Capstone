function transform(line) {
  function convertDateFormat(dateStr) {
    var parts = dateStr.split("-");
    // Assuming date format is dd-mm-yyyy
    return parts[2] + "-" + parts[1] + "-" + parts[0];
  }

  if (line.startsWith("order_number")) {
    return null; // Skip header line
  }

  var values = line.split(",").map(function (item) {
    return item.trim();
  });

  var obj = new Object();
  obj.order_number = parseInt(values[0]); // Assuming order_number is an integer
  obj.material_code = values[1];
  obj.quantity = parseInt(values[2]); // Assuming quantity is a decimal
  obj.start_date = convertDateFormat(values[3]);
  obj.end_date = convertDateFormat(values[4]);
  var jsonString = JSON.stringify(obj);
  return jsonString;
}
