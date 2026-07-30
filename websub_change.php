<?php
	include_once $_SERVER['DOCUMENT_ROOT'].'/env.php';
	header('Content-Type: application/json');
	$DefaultTable = 'ASADAL_CRAWLING_WEBSUB';
	$id = $_POST['id'];
	$pageNum = $_POST['pageNum'];
	$serch_page = $_POST['serch_page'];
	$serch_tex = $_POST['serch_tex'];
	$content_type = $_POST['content_type'];
	$like = $_POST['like'];
	$action = $_POST['action'];
	$chat_bot_id = $_POST['chat_bot_id'];
	$siteName = $_POST['siteName'];
	$domain = $_POST['domain'];
	$block = $_POST['block'];
	$where = "";
	if ($id && $id !== '') {
		// 여러 ID를 쉼표로 구분하여 받을 수 있도록 처리
		if (strpos($id, ',') !== false) {
			// 여러 ID인 경우 IN 쿼리 사용
			$idArray = array_map('trim', explode(',', $id));
			// PHP 버전 호환성을 위해 array_filter 대신 직접 필터링
			$sanitizedIds = array();
			foreach ($idArray as $val) {
				if (!empty($val) && is_numeric($val)) {
					$sanitizedIds[] = intval($val);
				}
			}
			if (!empty($sanitizedIds)) {
				// SQL injection 방지: 숫자만 허용하고 문자열로 감싸기 (기존 방식과 동일)
				$idList = "'" . implode("','", $sanitizedIds) . "'";
				$where .= " AND id IN (".$idList.")";
			}
		} else {
			// 단일 ID인 경우 기존 방식 완전히 유지 (문자열로 감싸서 비교)
			// 빈 문자열 체크는 위에서 이미 처리됨
			if (is_numeric($id)) {
				$where .= " AND id = '".intval($id)."'";
			} else {
				// 숫자가 아닌 경우도 기존 방식 유지 (혹시 모를 호환성)
				$where .= " AND id = '".addslashes($id)."'";
			}
		}
	}
	if ($chat_bot_id) {
		$where .= " AND chat_bot_id = '".addslashes($chat_bot_id)."'";
	}
	if ($serch_tex) {
		if ($content_type) {
			if ($like) {
				$where .= " AND ".sanitize_input($content_type)." LIKE '%".sanitize_input($serch_tex)."%'";
			} else {
				$where .= " AND ".sanitize_input($content_type)." = '".sanitize_input($serch_tex)."'";
			}
		} else {
			$where .= " AND (siteName LIKE '%".sanitize_input($serch_tex)."%' OR domain LIKE '%".sanitize_input($serch_tex)."%' OR block LIKE '%".sanitize_input($serch_tex)."%')";
		}
	}
	// 변수 초기화 (SQL 구문에서 사용될 변수들)
	$oder = " ORDER BY created_at DESC";
	$serch_pa = "";
	$offs = "";
	if ($serch_page) {
		$serch_pa = " LIMIT ".$serch_page;
	}
	if ($pageNum) {
		if ($pageNum == '0') {
			$offs = " OFFSET ".'0';
		} else {
			$offs = " OFFSET ".strval(intval($pageNum)*intval($serch_page));
		}
	}
	try {
		if ($action == 'list') {
			$allsql = "SELECT COUNT(*) as coun FROM {$DefaultTable} WHERE 1=1".$where;
			$dbObj->setQuery($allsql);
			$allres=$dbObj->exQuery();
			$row=mysql_fetch_assoc($allres);
			$sql = "SELECT * FROM `{$DefaultTable}` WHERE 1=1".$where.$oder.$serch_pa.$offs;
			$dbObj->setQuery($sql);
			$res=$dbObj->exQuery();
			$data = array();
			while($lrow=mysql_fetch_assoc($res)) {
				$data[] = $lrow;
			}
			echo json_encode(array(
				'coun'=>$row['coun'],
				'data'=>$data
			));
			exit;
		} else if ($action == 'insert') {
			$values = "('".sanitize_input($chat_bot_id)."','".sanitize_input($siteName)."','".sanitize_input($domain)."','".sanitize_input($block)."',now())";
			$insert_sql = "INSERT INTO {$DefaultTable} (chat_bot_id, sitename, domain, block, created_at) VALUES ".$values.";";
			$dbObj->setQuery($insert_sql);
			$dbObj->exQuery();
			echo json_encode(array('status'=>'success'));
			exit;
		} else if ($action == 'update') {
			$str = "";
			if ($sitename) {
				$str .= $str?" , ":"";
				$str .= "sitename = '".$sitename."'";
			}
			if ($domain) {
				$str .= $str?" , ":"";
				$str .= "domain = '".$domain."'";
			}
			if ($block) {
				$str .= $str?" , ":"";
				$str .= "block = '".$block."'";
			}
			$sql = "UPDATE {$DefaultTable} SET ".$str." WHERE id = '".$id."'";
			$dbObj->setQuery($sql);
			$dbObj->exQuery();
			echo json_encode(array('status'=>'success'));
			exit;
		} else if ($action == 'delete') {
			$sql = "DELETE FROM {$DefaultTable} WHERE id IN (".$id.")";
			$dbObj->setQuery($sql);
			$dbObj->exQuery();
			echo json_encode(array('status'=>'success'));
			exit;
		}
	} catch (Exception $e) {
		echo json_encode(array('message'=>'none','data'=>$e->getMessage()));
	}

	function sanitize_input($data) {
		return htmlspecialchars(trim($data), ENT_QUOTES, 'UTF-8');
	}
?>