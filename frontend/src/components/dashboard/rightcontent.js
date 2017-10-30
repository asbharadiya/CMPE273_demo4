import React, { Component } from 'react';
import {withRouter} from 'react-router-dom';
import NotificationSystem from 'react-notification-system';
import {connect} from 'react-redux';
import * as actions from '../../actions/asset';

class RightContent extends Component {

    constructor(props) {
        super(props);
        this.handleFileUpload = this.handleFileUpload.bind(this);
        this.notificationSystem = null;
    }

    componentDidMount(){
        this.notificationSystem = this.refs.notificationSystem;
    }

    componentWillReceiveProps(nextProps){
        if(nextProps.uploadFileSuccess){
            this.notificationSystem.addNotification({
              message: 'File successfully uploaded',
              level: 'success'
            });
        } else if(nextProps.uploadFileSuccess === false) {
            this.notificationSystem.addNotification({
              message: 'Opps! Something went wrong',
              level: 'error'
            });
        }
    }

    handleFileClick() {
        document.getElementById("fileUpload").value = null;
    }

    handleFileUpload() {
        let files = document.getElementById("fileUpload").files;
        this.props.addAsset(files[0]);   
    }

	render() {
        return (
        	<div className="right-content">
        		<div className="right-content-inner">
        			<div className="input-file-wrapper">
                        <button className="btn btn-primary btn-dropbox btn-main">Upload files</button>
                        <input type="file" id="fileUpload" name="upload" onChange={this.handleFileUpload} onClick={this.handleFileClick}/>
                    </div>
        		</div>
                <NotificationSystem ref="notificationSystem" />
          </div>
  	    );
	}
}

function mapStateToProps(state) {
    return {
        uploadFileSuccess:state.uploadFileSuccess
    };
}

function mapDispatchToProps(dispatch) {
    return {
        addAsset : (file) => dispatch(actions.addAsset(file))
    };
}

export default withRouter(connect(mapStateToProps,mapDispatchToProps)(props => <RightContent {...props}/>));
