<!DOCTYPE html>
<html lang="en">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <title>Autocomplete</title>
    <link rel="stylesheet" href="css/style.css" />
    <script type="text/javascript" src="js/jquery.min.js"></script>
    <script type="text/javascript" src="js/script.js"></script>
</head>

<body>
    <div class="container">
        <h1 class="title">Autocomplete</h1>
        <div class="content">
            <form>
                <div class="input_container">
                    <input type="text" id="words" onkeyup="autocomplet()">
                    <ul id="words_autocompleted"></ul>
                </div>
            </form>
        </div><!-- content -->
    </div><!-- container -->
</body>
</html>
