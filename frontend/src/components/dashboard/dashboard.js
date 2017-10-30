import React, { Component } from 'react';
import Header from './header';

class Dashboard extends Component {
  	render() {
      return (
      		<div className="dashboard-page">
      			<div className="page-content">
      				<Header/>
              {this.props.children}
      			</div>
      		</div>
    	);
  	}
}

export default Dashboard;
