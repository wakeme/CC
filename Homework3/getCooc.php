<?php
/**
 * @file getCooc.php
 * @author wakeme
 * @date 2016/06/16 02:27:53
 * @brief 
 *
 **/

define("TEST", "true");

$url = "http://localhost:8090/";
$word1 = isset($_POST['word1']) ? $_POST['word1'] : null;
$word2 = isset($_POST['word2']) ? $_POST['word2'] : null;

if ($word1 != null || $word2 != null)
{
    if ($word1 != null && $word2 != null) 
        $request = $word1 . "_" . $word2;
    else if ($word1 != null && $word2 == null) 
        $request = $word1 . "_*";
    else if ($word2 != null && $word1 == null) 
        $request = $word2 . "_*";
    $curl = ($url) . $request;

    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $curl);
    curl_setopt($ch, CURLOPT_HTTPHEADER , array("Accept:application/json"));
    curl_setopt($ch, CURLOPT_POST, 0);
    curl_setopt($ch, CURLOPT_NOBODY, 0);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    $curl_result = curl_exec($ch);
    curl_close($ch);
    // echo $curl_result;
    
    echo "<!DOCTYPE HTML>";
    echo "<head>";
    echo "<meta http-equiv=\"content-type\" content=\"text/html;charset=utf-8\">";
    echo "</head>";
    echo "<body>";
    echo "<table border=\"1\"><thead><tr><th>Word</th><th>Count</th></tr></thead><tbody>";
    $result = json_decode($curl_result, true);
    for ($x = 0; $x < count($result['Row']); $x++) {
        $row = $result['Row'][x];
        $row['key'] = base64_decode($row['key']);
        $len = count($row['Cell']);
        $row['Cell'][0]['$'] = base64_decode($row['Cell'][0]['$']);
        echo "<tr>";
        echo "<td>" + $row['key'] + "</td>";
        echo "<td>" + $row['Cell'][0]['$'] + "</td>";
        echo "</tr>";
    }
    echo "</tbody></table>";
    echo "</body>";
    exit;
    // echo json_encode($result);
}
    echo "<!DOCTYPE HTML>";
    echo "<head>";
    echo "<meta http-equiv=\"content-type\" content=\"text/html;charset=utf-8\">";
    echo "</head>";
    echo "<body>";
    echo "    <form action=\"\" method=\"POST\">";
    echo "        <table>";
    echo "            <tr>";
    echo "                <td><label>Word1</label></td>";
    echo "                <td><input type=\"text\" name=\"word1\" value=\"POST\" /></td>";
    echo "            </tr>";
    echo "            <tr>";
    echo "                <td><label>Word2</label></td>";
    echo "                <td><input type=\"text\" name=\"word2\" /></td>";
    echo "            </tr>";
    echo "            <tr><td><input type=\"submit\" value=\"Query\" /></td></tr>";
    echo "        </table>";
    echo "    </form>";
    echo "</body>";

 ?>
