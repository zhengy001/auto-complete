// autocomplet : this function will be executed every time we change the text
function autocomplet() {
	var min_length = 0; // min caracters to display the autocomplete
	var keyword = $('#words').val();
	if (keyword.length >= min_length) {
		$.ajax({
			url: 'ajax_refresh.php',
			type: 'POST',
			data: {keyword:keyword},
			success:function(data){
				$('#words_autocompleted').show();
				$('#words_autocompleted').html(data);
			}
		});
	} else {
		$('#words_autocomplete').hide();
	}
}

// set_item : this function will be executed when we select an item
function set_item(item) {
	// change input value
	$('#words').val(item);
	// hide proposition list
	$('#words_autocompleted').hide();
}