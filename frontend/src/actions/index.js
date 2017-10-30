import * as api from '../api/auth';

function checkSessionSuccess() {
  	return {type: "SESSION_ACTIVE"}
}

function checkSessionFailure(){
    return {type: "SESSION_INACTIVE"}
}

export function checkSession() {
	return function(dispatch) {
	    return api.checkSession().then(response => {
	    	if(response.status === 200){
	    		dispatch(checkSessionSuccess());
	    	} else {
	    		dispatch(checkSessionFailure());
	    	}
	    }).catch(error => {
	      	dispatch(checkSessionFailure());
	    });
	};
}

function logoutSuccess() {
  	return {type: "SESSION_INACTIVE"}
}

function logoutFailure(){
    return {type: "SESSION_INACTIVE"}
}

export function logout() {
	return function(dispatch) {
	    return api.logout().then(response => {
	    	if(response.status === 200){
	    		dispatch(logoutSuccess());
	    	} else {
	    		dispatch(logoutFailure());
	    	}
	    }).catch(error => {
	      	dispatch(logoutFailure());
	    });
	};
}

