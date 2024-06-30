function transform(line) {
    // Check if the line starts with the header
    if (line.startsWith("doc_number")) {
        return null; // Skip header line
    }

    var values = line.split(',').map(function(item) {
        return item.trim();
    });

    var obj = new Object();
    obj.doc_number = values[0];
    obj.item = values[1];
    obj.material_code = values[2];
    obj.price = parseFloat(values[3]); // Convert price to float
    obj.quantity = parseInt(values[4]); // Convert quantity to integer

    var jsonString = JSON.stringify(obj);
    return jsonString;
}
