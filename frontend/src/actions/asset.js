import * as api from '../api/asset';

function addAssetSuccess() {
  	return {type: "ADD_ASSET_SUCCESS"}
}

function addAssetFailure(){
    return {type: "ADD_ASSET_FAILURE"}
}

export function addAsset(file) {
	return function(dispatch) {
		let formData  = new FormData();
		if(file !== null) {
			formData.append("file",file);
		}
	    return api.addAsset(formData).then(response => {
	    	if(response.status === 200){
	    		dispatch(addAssetSuccess());
	    	} else {
	    		dispatch(addAssetFailure());
	    	}
	    }).catch(error => {
	      	dispatch(addAssetFailure());
	    });
	};
}

function getAssetsSuccess(data) {
  	return {type: "GET_ASSETS_SUCCESS",data}
}

function getAssetsFailure(){
    return {type: "GET_ASSETS_FAILURE"}
}

export function getAssets() {
	return function(dispatch) {
		return api.getAssets().then(response => {
	    	if(response.status === 200){
	    		dispatch(getAssetsSuccess(response.data));
	    	} else {
	    		dispatch(getAssetsFailure());
	    	}
	    }).catch(error => {
	      	dispatch(getAssetsFailure());
	    });
	};
}